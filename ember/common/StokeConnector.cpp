// The "stoke" connector (StokeConnector.h), in builds with stoke.

#include "ember/StokeConnector.h"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <optional>

#include <folly/coro/BlockingWait.h>
#include <folly/executors/GlobalExecutor.h>

#include "axiom/connectors/ConnectorMetadataRegistry.h"
#include "ember/Stoke.h"
#include "ember/shuffle/ChunkAppender.h"
#include "ember/shuffle/KilnShuffle.h"
#include "kiln/client/Client.h"
#include "stoke/client/Producer.h"
#include "stoke/client/Registry.h"
#include "stoke/client/StreamClient.h"
#include "stoke/format/Batch.h"
#include "velox/core/TableWriteTraits.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/DecodedVector.h"

namespace ember {

namespace velox = facebook::velox;
namespace axiom_connector = facebook::axiom::connector;
using folly::coro::blockingWait;

namespace {

constexpr std::string_view kBatchColumn = "batch";
constexpr std::string_view kTokensColumn = "tokens";

// Frames each batch straight into the chunk being written, through two
// buffers of the process's Kiln write pool (shuffle::ChunkAppender), and
// commits and puts the chunk once it has 'chunkBytes'. NULL rows are no
// batch. With packing, it packs the "tokens" of its rows into a batch in a
// buffer of the task's pool instead, and frames that once it is full; NULL
// rows add no tokens, NULL tokens are 0. The stream is opened per sink:
// a stream removed and created again under the same name gets a new bucket
// and maybe other shards.
class StokeDataSink : public velox::connector::DataSink {
 public:
  StokeDataSink(
      const StokeInsertTableHandle& handle,
      velox::memory::MemoryPool& pool)
      : chunkBytes_{handle.chunkBytes()},
        packing_{handle.packing()},
        producer_{
            blockingWait(stoke::StreamClient::co_open(
                std::make_shared<stoke::Registry>(
                    shuffle::kilnClient(handle.kilnMetadata())),
                handle.stream())),
            shuffle::kilnClient(handle.kilnMetadata())},
        pool_{shuffle::kilnWritePool(handle.kilnMetadata())} {
    if (packing_.has_value()) {
      window_ = packing_->sequence + 1;
      batch_ = velox::AlignedBuffer::allocate<uint8_t>(
          stoke::format::kHeaderSize +
              packing_->batch * window_ * sizeof(int32_t),
          &pool);
    }
  }

  // The drain uses the producer.
  ~StokeDataSink() override {
    if (drain_.valid()) {
      drain_.cancel();
      drain_.wait();
    }
  }

  void appendData(velox::RowVectorPtr input) override {
    if (packing_.has_value()) {
      pack(*input->childAt(1));
      return;
    }
    decoded_.decode(*input->childAt(0));
    for (velox::vector_size_t i = 0; i < input->size(); ++i) {
      if (!decoded_.isNullAt(i)) {
        const auto batch = decoded_.valueAt<velox::StringView>(i);
        write(batch.data(), batch.size());
      }
    }
  }

  // Waits until the stream consumed this task's chunks, so that the next
  // task in its slot starts within the budget (stoke::Producer). That can take
  // long, with slow trainers, so it gives the driver thread back between short
  // waits.
  bool finish() override {
    // The last batch: the whole windows left.
    if (filled_ >= window_ && window_ > 0) {
      writePacked(filled_ / window_);
    }
    filled_ = 0;
    put();
    if (!drain_.valid()) {
      drain_ =
          producer_.co_drain().semi().via(folly::getGlobalCPUExecutor());
    }
    drain_.wait(kDrainWait);
    if (!drain_.isReady()) {
      return false;
    }
    std::move(drain_).get();
    return true;
  }

  std::vector<std::string> close() override {
    return {};
  }

  void abort() override {
    appender_.reset();
    if (writer_ != nullptr) {
      try {
        blockingWait(writer_->co_abort());
      } catch (const std::exception&) {
      }
      writer_.reset();
    }
  }

  Stats stats() const override {
    return Stats{.numWrittenBytes = writtenBytes_};
  }

 private:
  // Frames 'batch' into the chunk being written, and puts the chunk once it
  // is full.
  void write(const char* batch, size_t size) {
    if (writer_ == nullptr) {
      writer_ = blockingWait(producer_.co_create(chunkBytes_));
      appender_.emplace(pool_, *writer_);
    }
    const auto length = stoke::format::frameLength(size);
    appender_->write(length.data(), length.size());
    appender_->write(batch, size);
    chunkSize_ += length.size() + size;
    if (chunkSize_ >= chunkBytes_) {
      put();
    }
  }

  // Appends the tokens of each row to 'batch_', and writes it when full.
  void pack(const velox::BaseVector& tokens) {
    decoded_.decode(tokens);
    const auto* arrays = decoded_.base()->as<velox::ArrayVector>();
    elements_.decode(*arrays->elements());
    const auto* flat = elements_.isIdentityMapping() && !elements_.mayHaveNulls()
        ? elements_.data<int32_t>()
        : nullptr;
    auto* payload = reinterpret_cast<int32_t*>(
        batch_->asMutable<uint8_t>() + stoke::format::kHeaderSize);
    const auto capacity = packing_->batch * window_;
    for (velox::vector_size_t i = 0; i < decoded_.size(); ++i) {
      if (decoded_.isNullAt(i)) {
        continue;
      }
      const auto row = decoded_.index(i);
      uint64_t offset = arrays->offsetAt(row);
      uint64_t size = arrays->sizeAt(row);
      while (size > 0) {
        const auto n = std::min(size, capacity - filled_);
        if (flat != nullptr) {
          std::memcpy(payload + filled_, flat + offset, n * sizeof(int32_t));
        } else {
          for (uint64_t k = 0; k < n; ++k) {
            const auto element = static_cast<velox::vector_size_t>(offset + k);
            payload[filled_ + k] = elements_.isNullAt(element)
                ? 0
                : elements_.valueAt<int32_t>(element);
          }
        }
        filled_ += n;
        offset += n;
        size -= n;
        if (filled_ == capacity) {
          writePacked(packing_->batch);
          filled_ = 0;
        }
      }
    }
  }

  // Writes the first 'rows' windows of 'batch_' as a batch.
  void writePacked(uint64_t rows) {
    const stoke::format::BatchHeader header{
        .dtype = stoke::format::DType::kInt32, .shape = {rows, window_}};
    const auto size = stoke::format::encodedSize(header);
    stoke::format::encodeHeader(header, {batch_->asMutable<uint8_t>(), size});
    write(batch_->as<char>(), size);
  }

  void put() {
    if (writer_ == nullptr) {
      return;
    }
    appender_->finish();
    appender_.reset();
    blockingWait(producer_.co_put(blockingWait(writer_->co_commit())));
    writer_.reset();
    writtenBytes_ += chunkSize_;
    chunkSize_ = 0;
  }

  static constexpr std::chrono::milliseconds kDrainWait{10};

  const uint64_t chunkBytes_;
  const std::optional<StokePacking> packing_;
  stoke::Producer producer_;
  folly::Future<folly::Unit> drain_ = folly::Future<folly::Unit>::makeEmpty();
  const std::shared_ptr<kiln::client::BufferPool> pool_;
  velox::DecodedVector decoded_;
  velox::DecodedVector elements_;
  // With packing: the batch being packed, its tokens so far and the tokens
  // in a window.
  velox::BufferPtr batch_;
  uint64_t filled_{0};
  uint64_t window_{0};
  // The chunk being written, if any, and its bytes so far.
  std::unique_ptr<kiln::client::ChunkWriter> writer_;
  std::optional<shuffle::ChunkAppender> appender_;
  uint64_t chunkSize_{0};
  uint64_t writtenBytes_{0};
};

// Streams are only written: the layout has no scans.
class StokeTableLayout : public axiom_connector::TableLayout {
 public:
  StokeTableLayout(
      const axiom_connector::Table* table,
      velox::connector::Connector* connector,
      std::vector<const axiom_connector::Column*> columns)
      : TableLayout(
            "stoke",
            table,
            connector,
            std::move(columns),
            /*partitionColumns=*/{},
            /*orderColumns=*/{},
            /*sortOrder=*/{},
            /*lookupKeys=*/{},
            /*supportsScan=*/false) {}

  velox::connector::ColumnHandlePtr createColumnHandle(
      const axiom_connector::ConnectorSessionPtr& /*session*/,
      const std::string& /*columnName*/,
      std::vector<velox::common::Subfield> /*subfields*/,
      std::optional<velox::TypePtr> /*castToType*/,
      axiom_connector::SubfieldMapping /*subfieldMapping*/) const override {
    VELOX_UNSUPPORTED("stoke streams are read by stoke's dataloader");
  }

  velox::connector::ConnectorTableHandlePtr createTableHandle(
      const axiom_connector::ConnectorSessionPtr& /*session*/,
      std::vector<velox::connector::ColumnHandlePtr> /*columnHandles*/,
      velox::core::ExpressionEvaluator& /*evaluator*/,
      std::vector<velox::core::TypedExprPtr> /*filters*/,
      std::vector<int32_t>& /*rejectedFilterIndices*/,
      velox::RowTypePtr /*dataColumns*/,
      std::optional<axiom_connector::LookupKeys> /*lookupKeys*/)
      const override {
    VELOX_UNSUPPORTED("stoke streams are read by stoke's dataloader");
  }
};

class StokeTable : public axiom_connector::Table {
 public:
  StokeTable(
      facebook::axiom::SchemaTableName name,
      velox::connector::Connector* connector)
      : Table(
            std::move(name),
            makeColumns(velox::ROW(
                {std::string(kBatchColumn), std::string(kTokensColumn)},
                {velox::VARBINARY(), velox::ARRAY(velox::INTEGER())}))),
        layout_{this, connector, allColumns()},
        layouts_{&layout_} {}

  const std::vector<const axiom_connector::TableLayout*>& layouts()
      const override {
    return layouts_;
  }

  std::optional<uint64_t> numRows() const override {
    return std::nullopt;
  }

 private:
  const StokeTableLayout layout_;
  const std::vector<const axiom_connector::TableLayout*> layouts_;
};

std::string requiredProperty(
    const axiom_connector::ConnectorSessionPtr& session,
    std::string_view name) {
  auto value = session ? session->property(name) : std::nullopt;
  VELOX_USER_CHECK(
      value.has_value() && !value->empty(),
      "The stoke connector needs the session property {}",
      name);
  return std::string(*value);
}

// B and T of a write of "tokens", if set.
std::optional<StokePacking> packing(
    const axiom_connector::ConnectorSessionPtr& session) {
  auto batch = session ? session->property(kStokePackBatch) : std::nullopt;
  auto sequence =
      session ? session->property(kStokePackSequence) : std::nullopt;
  VELOX_USER_CHECK_EQ(
      batch.has_value(),
      sequence.has_value(),
      "Set both {} and {}, or neither",
      kStokePackBatch,
      kStokePackSequence);
  if (!batch.has_value()) {
    return std::nullopt;
  }
  StokePacking packing{
      folly::to<uint64_t>(*batch), folly::to<uint64_t>(*sequence)};
  VELOX_USER_CHECK(
      packing.batch > 0 && packing.sequence > 0,
      "Packing needs a positive batch size and sequence length");
  return packing;
}

uint64_t chunkBytes(const axiom_connector::ConnectorSessionPtr& session) {
  auto value = session ? session->property(kStokeChunkBytes) : std::nullopt;
  if (!value.has_value()) {
    return kDefaultStokeChunkBytes;
  }
  const auto bytes = folly::to<uint64_t>(*value);
  VELOX_USER_CHECK_GT(bytes, 0, "{} must be positive", kStokeChunkBytes);
  return bytes;
}

} // namespace

StokeInsertTableHandle::StokeInsertTableHandle(
    std::string stream,
    std::string kilnMetadata,
    uint64_t chunkBytes,
    std::optional<StokePacking> packing)
    : stream_{std::move(stream)},
      kilnMetadata_{std::move(kilnMetadata)},
      chunkBytes_{chunkBytes},
      packing_{packing} {}

std::string StokeInsertTableHandle::toString() const {
  return fmt::format(
      "stoke stream {} in {}, chunks of {} bytes{}",
      stream_,
      kilnMetadata_,
      chunkBytes_,
      packing_.has_value()
          ? fmt::format(
                ", packed into batches of {} x {}",
                packing_->batch,
                packing_->sequence + 1)
          : "");
}

folly::dynamic StokeInsertTableHandle::serialize() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["name"] = "StokeInsertTableHandle";
  obj["stream"] = stream_;
  obj["kilnMetadata"] = kilnMetadata_;
  obj["chunkBytes"] = static_cast<int64_t>(chunkBytes_);
  if (packing_.has_value()) {
    obj["packBatch"] = static_cast<int64_t>(packing_->batch);
    obj["packSequence"] = static_cast<int64_t>(packing_->sequence);
  }
  return obj;
}

std::shared_ptr<StokeInsertTableHandle> StokeInsertTableHandle::create(
    const folly::dynamic& obj) {
  std::optional<StokePacking> packing;
  if (obj.count("packBatch") != 0) {
    packing = StokePacking{
        static_cast<uint64_t>(obj["packBatch"].asInt()),
        static_cast<uint64_t>(obj["packSequence"].asInt())};
  }
  return std::make_shared<StokeInsertTableHandle>(
      obj["stream"].asString(),
      obj["kilnMetadata"].asString(),
      static_cast<uint64_t>(obj["chunkBytes"].asInt()),
      packing);
}

void StokeInsertTableHandle::registerSerDe() {
  velox::DeserializationRegistryForSharedPtr().Register(
      "StokeInsertTableHandle", StokeInsertTableHandle::create);
}

std::unique_ptr<velox::connector::DataSource> StokeConnector::createDataSource(
    const velox::RowTypePtr& /*outputType*/,
    const velox::connector::ConnectorTableHandlePtr& /*tableHandle*/,
    const velox::connector::ColumnHandleMap& /*columnHandles*/,
    velox::connector::ConnectorQueryCtx* /*connectorQueryCtx*/) {
  VELOX_UNSUPPORTED("stoke streams are read by stoke's dataloader");
}

std::unique_ptr<velox::connector::DataSink> StokeConnector::createDataSink(
    velox::RowTypePtr inputType,
    velox::connector::ConnectorInsertTableHandlePtr handle,
    velox::connector::ConnectorQueryCtx* connectorQueryCtx,
    velox::connector::CommitStrategy /*commitStrategy*/) {
  VELOX_USER_CHECK(
      inputType->size() == 2 && inputType->childAt(0)->isVarbinary() &&
          *inputType->childAt(1) == *velox::ARRAY(velox::INTEGER()),
      "A stoke stream takes batches (VARBINARY) or tokens (ARRAY<INTEGER>), "
      "not {}",
      inputType->toString());
  return std::make_unique<StokeDataSink>(
      *handle->asChecked<StokeInsertTableHandle>(),
      *connectorQueryCtx->memoryPool());
}

axiom_connector::TablePtr StokeConnectorMetadata::findTable(
    const facebook::axiom::SchemaTableName& tableName) {
  return std::make_shared<StokeTable>(tableName, connector_);
}

axiom_connector::ConnectorWriteHandlePtr StokeConnectorMetadata::beginWrite(
    const axiom_connector::ConnectorSessionPtr& session,
    const axiom_connector::TablePtr& table,
    axiom_connector::WriteKind kind,
    const velox::connector::ConnectorTableHandlePtr& /*scanHandle*/,
    bool explain) {
  VELOX_USER_CHECK(
      kind == axiom_connector::WriteKind::kInsert,
      "stoke streams only take inserts");
  const auto& stream = table->name().table;
  auto kilnMetadata = requiredProperty(session, kStokeKilnMetadata);
  if (!explain) {
    stoke::Registry registry(shuffle::kilnClient(kilnMetadata));
    auto info = blockingWait(registry.co_get(stream));
    VELOX_USER_CHECK(info.has_value(), "No stoke stream {}", stream);
    VELOX_USER_CHECK(
        info->state == stoke::StreamState::kOpen,
        "The stoke stream {} is sealed",
        stream);
  }
  return std::make_shared<axiom_connector::ConnectorWriteHandle>(
      std::make_shared<StokeInsertTableHandle>(
          stream,
          std::move(kilnMetadata),
          chunkBytes(session),
          packing(session)),
      velox::core::TableWriteTraits::outputType(std::nullopt));
}

axiom_connector::RowsFuture StokeConnectorMetadata::finishWrite(
    const axiom_connector::ConnectorSessionPtr& session,
    const axiom_connector::ConnectorWriteHandlePtr& handle,
    const std::vector<velox::RowVectorPtr>& writeResults,
    velox::RowVectorPtr /*groupingKeys*/,
    std::vector<std::vector<axiom_connector::ColumnStatistics>> /*groupStats*/) {
  int64_t rows = 0;
  velox::DecodedVector decoded;
  for (const auto& result : writeResults) {
    decoded.decode(
        *result->childAt(velox::core::TableWriteTraits::kRowCountChannel));
    for (velox::vector_size_t i = 0; i < decoded.size(); ++i) {
      if (!decoded.isNullAt(i)) {
        rows += decoded.valueAt<int64_t>(i);
      }
    }
  }
  if (session != nullptr && session->property(kStokeSeal) == "true") {
    const auto* insert =
        handle->veloxHandle()->asChecked<StokeInsertTableHandle>();
    stoke::Registry registry(shuffle::kilnClient(insert->kilnMetadata()));
    blockingWait(registry.co_seal(insert->stream()));
  }
  return folly::makeSemiFuture(std::optional<int64_t>{rows});
}

std::vector<std::string> StokeConnectorMetadata::listSchemaNames(
    const axiom_connector::ConnectorSessionPtr& /*session*/) {
  return {};
}

bool StokeConnectorMetadata::schemaExists(
    const axiom_connector::ConnectorSessionPtr& /*session*/,
    const std::string& /*schemaName*/) {
  return true;
}

std::vector<std::string> StokeConnectorMetadata::listTableNames(
    const axiom_connector::ConnectorSessionPtr& /*session*/,
    const std::string& /*schemaName*/) {
  VELOX_UNSUPPORTED("stoke streams are listed by stoke ls");
}

void registerStokeConnector() {
  StokeInsertTableHandle::registerSerDe();
  auto connector = std::make_shared<StokeConnector>();
  velox::connector::registerConnector(connector);
  axiom_connector::ConnectorMetadataRegistry::global().insert(
      connector->connectorId(),
      std::make_shared<StokeConnectorMetadata>(connector.get()));
}

} // namespace ember
