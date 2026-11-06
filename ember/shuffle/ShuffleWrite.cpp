#include "ember/shuffle/ShuffleWrite.h"

#include "ember/shuffle/BackgroundWork.h"
#include "ember/shuffle/FileShuffle.h"
#include "ember/shuffle/KilnShuffle.h"
#include "ember/shuffle/ShuffleRuntime.h"
#include "velox/common/memory/ByteStream.h"
#include "velox/exec/OperatorUtils.h"
#include "velox/exec/Task.h"
#include "velox/vector/VectorStream.h"

namespace ember::shuffle {

namespace core = facebook::velox::core;
namespace exec = facebook::velox::exec;
namespace velox = facebook::velox;

ShuffleWriteNode::ShuffleWriteNode(
    const core::PlanNodeId& id,
    int32_t shuffleId,
    Kind kind,
    std::vector<core::TypedExprPtr> keys,
    int32_t numPartitions,
    core::PartitionFunctionSpecPtr partitionFunctionSpec,
    velox::RowTypePtr outputType,
    std::string serdeKind,
    ShuffleTarget target,
    core::PlanNodePtr source)
    : PlanNode(id),
      shuffleId_{shuffleId},
      kind_{kind},
      keys_{std::move(keys)},
      numPartitions_{numPartitions},
      partitionFunctionSpec_{std::move(partitionFunctionSpec)},
      outputType_{std::move(outputType)},
      serdeKind_{std::move(serdeKind)},
      target_{std::move(target)},
      sources_{std::move(source)} {
  VELOX_USER_CHECK_GT(numPartitions_, 0);
  VELOX_USER_CHECK_EQ(
      serdeKind_, "Presto", "ShuffleWrite supports only the Presto serde");
  if (kind_ == Kind::kBroadcast) {
    VELOX_USER_CHECK_EQ(numPartitions_, 1, "A broadcast writes one partition");
  }
  if (kind_ == Kind::kPartitioned && numPartitions_ > 1) {
    VELOX_USER_CHECK_NOT_NULL(partitionFunctionSpec_);
  }
}

std::shared_ptr<const ShuffleWriteNode> ShuffleWriteNode::fromPartitionedOutput(
    const core::PartitionedOutputNode& output,
    int32_t shuffleId,
    int32_t numPartitions,
    ShuffleTarget target) {
  VELOX_USER_CHECK(
      !output.isReplicateNullsAndAny(),
      "Shuffle does not support replicating nulls: {}",
      output.toString());
  return std::make_shared<const ShuffleWriteNode>(
      output.id(),
      shuffleId,
      output.kind(),
      output.keys(),
      output.isBroadcast() ? 1 : numPartitions,
      output.partitionFunctionSpecPtr(),
      output.outputType(),
      output.serdeKind(),
      std::move(target),
      output.sources()[0]);
}

std::shared_ptr<const ShuffleWriteNode> ShuffleWriteNode::withShuffleId(
    int32_t shuffleId) const {
  return std::make_shared<const ShuffleWriteNode>(
      id(),
      shuffleId,
      kind_,
      keys_,
      numPartitions_,
      partitionFunctionSpec_,
      outputType_,
      serdeKind_,
      target_,
      sources_[0]);
}

std::shared_ptr<const ShuffleWriteNode> ShuffleWriteNode::withPartitionFunction(
    core::PartitionFunctionSpecPtr spec) const {
  return std::make_shared<const ShuffleWriteNode>(
      id(),
      shuffleId_,
      kind_,
      keys_,
      numPartitions_,
      std::move(spec),
      outputType_,
      serdeKind_,
      target_,
      sources_[0]);
}

std::shared_ptr<const ShuffleWriteNode> ShuffleWriteNode::withKilnBucket(
    uint64_t bucket) const {
  auto target = target_;
  target.kilnBucket = bucket;
  return std::make_shared<const ShuffleWriteNode>(
      id(),
      shuffleId_,
      kind_,
      keys_,
      numPartitions_,
      partitionFunctionSpec_,
      outputType_,
      serdeKind_,
      std::move(target),
      sources_[0]);
}

folly::dynamic ShuffleWriteNode::serialize() const {
  auto obj = PlanNode::serialize();
  obj["shuffleId"] = shuffleId_;
  obj["kind"] = static_cast<int32_t>(kind_);
  obj["keys"] = velox::ISerializable::serialize(keys_);
  obj["numPartitions"] = numPartitions_;
  if (partitionFunctionSpec_ != nullptr) {
    obj["partitionFunctionSpec"] = partitionFunctionSpec_->serialize();
  }
  obj["outputType"] = outputType_->serialize();
  obj["serdeKind"] = serdeKind_;
  obj["target"] = target_.serialize();
  return obj;
}

core::PlanNodePtr ShuffleWriteNode::create(
    const folly::dynamic& obj,
    void* context) {
  core::PartitionFunctionSpecPtr spec;
  if (obj.count("partitionFunctionSpec")) {
    spec = velox::ISerializable::deserialize<core::PartitionFunctionSpec>(
        obj["partitionFunctionSpec"], context);
  }
  auto sources = velox::ISerializable::deserialize<std::vector<core::PlanNode>>(
      obj["sources"], context);
  VELOX_CHECK_EQ(sources.size(), 1);
  return std::make_shared<const ShuffleWriteNode>(
      obj["id"].asString(),
      obj["shuffleId"].asInt(),
      static_cast<Kind>(obj["kind"].asInt()),
      velox::ISerializable::deserialize<std::vector<core::ITypedExpr>>(
          obj["keys"], context),
      obj["numPartitions"].asInt(),
      std::move(spec),
      velox::ISerializable::deserialize<velox::RowType>(obj["outputType"]),
      obj["serdeKind"].asString(),
      ShuffleTarget::create(obj["target"]),
      sources[0]);
}

void ShuffleWriteNode::addDetails(std::stringstream& stream) const {
  stream << "shuffle " << shuffleId_ << ", "
         << core::PartitionedOutputNode::toName(kind_) << " "
         << numPartitions_;
  if (!keys_.empty()) {
    stream << " (";
    for (size_t i = 0; i < keys_.size(); ++i) {
      stream << (i > 0 ? ", " : "") << keys_[i]->toString();
    }
    stream << ")";
  }
}

namespace {

// Pages are cut once a partition has this many serialized bytes.
constexpr int64_t kTargetPageBytes = 1 << 20;

class ShuffleWrite : public exec::Operator {
 public:
  ShuffleWrite(
      int32_t operatorId,
      exec::DriverCtx* ctx,
      const std::shared_ptr<const ShuffleWriteNode>& node)
      : Operator(ctx, node->outputType(), operatorId, node->id(), "ShuffleWrite"),
        node_{node},
        serde_{velox::getNamedVectorSerde(node->serdeKind())},
        serdeOptions_{exec::getVectorSerdeOptions(
            velox::common::stringToCompressionKind(
                ctx->queryConfig().shuffleCompressionKind()),
            node->serdeKind(),
            std::nullopt,
            ctx->queryConfig().minShuffleCompressionPageSizeBytes())},
        outputChannels_{exec::calculateOutputChannels(
            node->sources()[0]->outputType(),
            node->outputType(),
            node->outputType())},
        partitions_(node->numPartitions()) {
    if (node->kind() == ShuffleWriteNode::Kind::kPartitioned &&
        node->numPartitions() > 1) {
      partitionFunction_ = node->partitionFunctionSpec()->create(
          node->numPartitions(), /*localExchange=*/false);
    }
  }

  void initialize() override {
    Operator::initialize();
    auto* task = operatorCtx_->task().get();
    const auto runtime = ShuffleRuntime::current();
    auto store = runtime->store();
    work_ = std::make_unique<BackgroundWork>(runtime->ioExecutor());
    const auto& target = node_->target();
    if (target.store == "kiln") {
      writer_ = createKilnWriter(
          std::move(store),
          target.kilnMetadata,
          target.kilnTier,
          target.kilnBucket,
          node_->shuffleId(),
          task->destination(),
          node_->numPartitions(),
          attemptTag(task->taskId()),
          target.spill);
    } else {
      writer_ = store->createWriter(
          node_->shuffleId(),
          task->destination(),
          node_->numPartitions(),
          attemptTag(task->taskId()));
    }
  }

  bool needsInput() const override {
    return !noMoreInput_;
  }

  // Needs no spill_enabled: the writer's run files are not Velox spill files.
  bool canReclaim() const override {
    return true;
  }

  // Serializes into the spill pool: the operator's pool is the one out of
  // memory.
  void reclaim(
      uint64_t /*targetBytes*/,
      velox::memory::MemoryReclaimer::Stats& /*stats*/) override {
    VELOX_CHECK(!nonReclaimableSection_);
    // Arbitration can arrive after close(). A failed write is the driver's to
    // report.
    if (work_ == nullptr || partitions_.empty() || !work_->finish()) {
      return;
    }
    if (bufferedBytes() == 0) {
      return;
    }
    // Reclaim leaves writing to this thread: the task is paused until it is
    // done.
    flushAll(*velox::memory::spillMemoryPool());
    writer_->spill();
    pageBytes_ = 0;
    ++reclaimSpills_;
    pool()->release();
  }

  void addInput(velox::RowVectorPtr input) override {
    auto output = project(input);
    const auto numRows = output->size();
    if (numRows == 0) {
      return;
    }
    for (auto i = 0; i < output->childrenSize(); ++i) {
      output->childAt(i)->loadedVector();
    }
    // Room for the input and for copying a finished page out.
    reserve(output->estimateFlatSize() + kTargetPageBytes);

    const auto numPartitions = node_->numPartitions();
    for (auto& rows : rows_) {
      rows.clear();
    }
    rows_.resize(numPartitions);
    std::optional<uint32_t> single;
    if (partitionFunction_ != nullptr) {
      partitionIds_.resize(numRows);
      single = partitionFunction_->partition(*output, partitionIds_);
    } else if (node_->kind() == ShuffleWriteNode::Kind::kArbitrary) {
      single = nextArbitrary_;
      nextArbitrary_ = (nextArbitrary_ + 1) % numPartitions;
    } else {
      single = 0;
    }

    if (single.has_value()) {
      append(*single, output, allRows(numRows));
    } else {
      for (velox::vector_size_t row = 0; row < numRows; ++row) {
        rows_[partitionIds_[row]].push_back(row);
      }
      for (int32_t p = 0; p < numPartitions; ++p) {
        if (!rows_[p].empty()) {
          append(p, output, folly::Range(rows_[p].data(), rows_[p].size()));
        }
      }
    }
    if (bufferedBytes() >= node_->target().spillThreshold) {
      flushAll(*pool());
      pageBytes_ = 0;
      ++thresholdSpills_;
      work_->start([this] { writer_->spill(); });
    }
  }

  velox::RowVectorPtr getOutput() override {
    return nullptr;
  }

  void noMoreInput() override {
    Operator::noMoreInput();
    reserve(kTargetPageBytes);
    flushAll(*pool());
    pageBytes_ = 0;
    work_->start([this] {
      writer_->commit();
      committed_ = true;
      auto lockedStats = stats_.wlock();
      lockedStats->addRuntimeStat(
          "shuffleBytesWritten",
          velox::RuntimeCounter(
              writer_->bytesWritten(), velox::RuntimeCounter::Unit::kBytes));
      lockedStats->addRuntimeStat(
          "shuffleThresholdSpills", velox::RuntimeCounter(thresholdSpills_));
      lockedStats->addRuntimeStat(
          "shuffleReclaimSpills", velox::RuntimeCounter(reclaimSpills_));
    });
  }

  exec::BlockingReason isBlocked(velox::ContinueFuture* future) override {
    return work_ != nullptr && work_->blocked(future)
        ? exec::BlockingReason::kWaitForConsumer
        : exec::BlockingReason::kNotBlocked;
  }

  bool isFinished() override {
    return committed_;
  }

  void close() override {
    if (work_ != nullptr) {
      work_->wait();
    }
    if (writer_ != nullptr && !committed_) {
      writer_->abort();
    }
    partitions_.clear();
    Operator::close();
  }

 private:
  struct Partition {
    std::unique_ptr<velox::VectorStreamGroup> group;
    velox::vector_size_t numRows{0};
    int64_t bytes{0};
  };

  // Task ids may contain '/' and ':'; keep file names flat.
  static std::string attemptTag(const std::string& taskId) {
    std::string tag = taskId;
    for (auto& c : tag) {
      if (!std::isalnum(static_cast<unsigned char>(c)) && c != '.' &&
          c != '-' && c != '_') {
        c = '_';
      }
    }
    return tag;
  }

  folly::Range<const velox::vector_size_t*> allRows(
      velox::vector_size_t numRows) {
    if (allRows_.size() < numRows) {
      allRows_.resize(numRows);
      std::iota(allRows_.begin(), allRows_.end(), 0);
    }
    return folly::Range(allRows_.data(), numRows);
  }

  velox::RowVectorPtr project(const velox::RowVectorPtr& input) const {
    if (outputChannels_.empty()) {
      return input;
    }
    std::vector<velox::VectorPtr> children;
    children.reserve(outputChannels_.size());
    for (auto channel : outputChannels_) {
      children.push_back(input->childAt(channel));
    }
    return std::make_shared<velox::RowVector>(
        input->pool(), outputType_, nullptr, input->size(), std::move(children));
  }

  void append(
      int32_t partition,
      const velox::RowVectorPtr& output,
      folly::Range<const velox::vector_size_t*> rows) {
    auto& state = partitions_[partition];
    if (state.group == nullptr) {
      state.group = std::make_unique<velox::VectorStreamGroup>(pool(), serde_);
      state.group->createStreamTree(
          outputType_, rows.size(), serdeOptions_.get());
    }
    state.group->append(output, rows, scratch_);
    state.numRows += rows.size();
    const int64_t bytes = state.group->size();
    groupBytes_ += bytes - state.bytes;
    state.bytes = bytes;
    if (bytes >= kTargetPageBytes) {
      flush(partition, *pool());
    }
  }

  // What the spill threshold counts: the pages at the writer and the open
  // partition groups.
  int64_t bufferedBytes() const {
    return pageBytes_ + groupBytes_;
  }

  // The driver keeps its operators out of reclaim while it calls them, so this
  // is where the arbitrator can make this operator give its memory back.
  void reserve(int64_t bytes) {
    if (pool()->availableReservation() < bytes) {
      Operator::ReclaimableSectionGuard guard(this);
      pool()->maybeReserve(bytes);
    }
  }

  void flushAll(velox::memory::MemoryPool& pagePool) {
    for (int32_t p = 0; p < partitions_.size(); ++p) {
      flush(p, pagePool);
    }
  }

  // Serializes the partition's rows into a page allocated from 'pagePool' and
  // hands it to the writer.
  void flush(int32_t partition, velox::memory::MemoryPool& pagePool) {
    auto& state = partitions_[partition];
    if (state.group == nullptr || state.numRows == 0) {
      return;
    }
    constexpr int32_t kMinMessageSize = 128;
    velox::IOBufOutputStream stream(
        pagePool,
        nullptr,
        std::max<int64_t>(kMinMessageSize, state.group->size()));
    state.group->flush(&stream);
    auto page = stream.getIOBuf();
    pageBytes_ += page->computeChainDataLength();
    writer_->collect(partition, std::move(page));
    groupBytes_ -= state.bytes;
    state.bytes = 0;
    state.group.reset();
    state.numRows = 0;
  }

  const std::shared_ptr<const ShuffleWriteNode> node_;
  velox::VectorSerde* const serde_;
  const std::unique_ptr<velox::VectorSerde::Options> serdeOptions_;
  const std::vector<velox::column_index_t> outputChannels_;
  std::unique_ptr<core::PartitionFunction> partitionFunction_;
  std::unique_ptr<ShuffleWriter> writer_;
  std::vector<Partition> partitions_;
  std::vector<uint32_t> partitionIds_;
  std::vector<std::vector<velox::vector_size_t>> rows_;
  std::vector<velox::vector_size_t> allRows_;
  velox::Scratch scratch_;
  uint32_t nextArbitrary_{0};
  int64_t pageBytes_{0};
  int64_t groupBytes_{0};
  int64_t thresholdSpills_{0};
  int64_t reclaimSpills_{0};
  std::atomic<bool> committed_{false};
  std::unique_ptr<BackgroundWork> work_;
};

} // namespace

std::unique_ptr<exec::Operator> ShuffleWriteTranslator::toOperator(
    exec::DriverCtx* ctx,
    int32_t id,
    const core::PlanNodePtr& node) {
  if (auto write = std::dynamic_pointer_cast<const ShuffleWriteNode>(node)) {
    return std::make_unique<ShuffleWrite>(id, ctx, write);
  }
  return nullptr;
}

} // namespace ember::shuffle
