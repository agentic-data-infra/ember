#include "ember/shuffle/ShuffleWrite.h"

#include "ember/shuffle/FileShuffle.h"
#include "ember/shuffle/ShuffleConfig.h"
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
    core::PlanNodePtr source)
    : PlanNode(id),
      shuffleId_{shuffleId},
      kind_{kind},
      keys_{std::move(keys)},
      numPartitions_{numPartitions},
      partitionFunctionSpec_{std::move(partitionFunctionSpec)},
      outputType_{std::move(outputType)},
      serdeKind_{std::move(serdeKind)},
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
    int32_t numPartitions) {
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
      output.sources()[0]);
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
    const auto& config = operatorCtx_->driverCtx()->queryConfig();
    auto location = config.get<std::string>(std::string(kExecutorLocation));
    VELOX_CHECK(
        location.has_value(),
        "ShuffleWrite needs config {}",
        kExecutorLocation);
    auto* task = operatorCtx_->task().get();
    writer_ = ShuffleBlockStore::lookup(*location)->createWriter(
        node_->shuffleId(),
        task->destination(),
        node_->numPartitions(),
        attemptTag(task->taskId()),
        config.get<int64_t>(
            std::string(kSpillThreshold),
            ShuffleBlockStore::kDefaultSpillThreshold));
  }

  bool needsInput() const override {
    return !noMoreInput_;
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
  }

  velox::RowVectorPtr getOutput() override {
    return nullptr;
  }

  void noMoreInput() override {
    Operator::noMoreInput();
    for (int32_t p = 0; p < partitions_.size(); ++p) {
      flush(p);
    }
    writer_->commit();
    committed_ = true;
    auto lockedStats = stats_.wlock();
    lockedStats->addRuntimeStat(
        "shuffleBytesWritten",
        velox::RuntimeCounter(
            writer_->bytesWritten(), velox::RuntimeCounter::Unit::kBytes));
    lockedStats->addRuntimeStat(
        "shuffleSpills", velox::RuntimeCounter(writer_->spillCount()));
  }

  exec::BlockingReason isBlocked(velox::ContinueFuture* /*future*/) override {
    return exec::BlockingReason::kNotBlocked;
  }

  bool isFinished() override {
    return committed_;
  }

  void close() override {
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
    if (state.group->size() >= kTargetPageBytes) {
      flush(partition);
    }
  }

  void flush(int32_t partition) {
    auto& state = partitions_[partition];
    if (state.group == nullptr || state.numRows == 0) {
      return;
    }
    constexpr int32_t kMinMessageSize = 128;
    velox::IOBufOutputStream stream(
        *pool(),
        nullptr,
        std::max<int64_t>(kMinMessageSize, state.group->size()));
    state.group->flush(&stream);
    writer_->collect(partition, stream.getIOBuf());
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
  bool committed_{false};
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
