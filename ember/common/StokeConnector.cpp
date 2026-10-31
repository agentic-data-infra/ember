// The "stoke" connector (StokeConnector.h), in builds with stoke.

#include "ember/StokeConnector.h"

#include <folly/coro/BlockingWait.h>

#include "axiom/connectors/ConnectorMetadataRegistry.h"
#include "ember/Stoke.h"
#include "ember/shuffle/KilnShuffle.h"
#include "kiln/client/Client.h"
#include "stoke/client/Producer.h"
#include "stoke/client/Registry.h"
#include "stoke/client/StreamClient.h"
#include "stoke/format/Batch.h"
#include "velox/core/TableWriteTraits.h"
#include "velox/vector/DecodedVector.h"

namespace ember {

namespace velox = facebook::velox;
namespace axiom_connector = facebook::axiom::connector;
using folly::coro::blockingWait;

namespace {

constexpr std::string_view kBatchColumn = "batch";

// Frames each batch into the chunk being built and puts the chunk once it
// has 'chunkBytes'. NULL rows are no batch. The stream is opened per sink:
// a stream removed and created again under the same name gets a new bucket
// and maybe other shards.
class StokeDataSink : public velox::connector::DataSink {
 public:
  explicit StokeDataSink(const StokeInsertTableHandle& handle)
      : chunkBytes_{handle.chunkBytes()},
        producer_{
            blockingWait(stoke::StreamClient::co_open(
                std::make_shared<stoke::Registry>(
                    shuffle::kilnClient(handle.kilnMetadata())),
                handle.stream())),
            shuffle::kilnClient(handle.kilnMetadata())} {}

  void appendData(velox::RowVectorPtr input) override {
    decoded_.decode(*input->childAt(0));
    for (velox::vector_size_t i = 0; i < input->size(); ++i) {
      if (decoded_.isNullAt(i)) {
        continue;
      }
      const auto batch = decoded_.valueAt<velox::StringView>(i);
      stoke::format::appendFrame(chunk_, std::string_view(batch));
      if (chunk_.size() >= chunkBytes_) {
        put();
      }
    }
  }

  bool finish() override {
    put();
    return true;
  }

  std::vector<std::string> close() override {
    return {};
  }

  void abort() override {
    chunk_.clear();
  }

  Stats stats() const override {
    return Stats{.numWrittenBytes = writtenBytes_};
  }

 private:
  void put() {
    if (chunk_.empty()) {
      return;
    }
    blockingWait(producer_.co_put(chunk_));
    writtenBytes_ += chunk_.size();
    chunk_.clear();
  }

  const uint64_t chunkBytes_;
  stoke::Producer producer_;
  velox::DecodedVector decoded_;
  std::string chunk_;
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
            makeColumns(
                velox::ROW({std::string(kBatchColumn)}, {velox::VARBINARY()}))),
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
    uint64_t chunkBytes)
    : stream_{std::move(stream)},
      kilnMetadata_{std::move(kilnMetadata)},
      chunkBytes_{chunkBytes} {}

std::string StokeInsertTableHandle::toString() const {
  return fmt::format(
      "stoke stream {} in {}, chunks of {} bytes",
      stream_,
      kilnMetadata_,
      chunkBytes_);
}

folly::dynamic StokeInsertTableHandle::serialize() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["name"] = "StokeInsertTableHandle";
  obj["stream"] = stream_;
  obj["kilnMetadata"] = kilnMetadata_;
  obj["chunkBytes"] = static_cast<int64_t>(chunkBytes_);
  return obj;
}

std::shared_ptr<StokeInsertTableHandle> StokeInsertTableHandle::create(
    const folly::dynamic& obj) {
  return std::make_shared<StokeInsertTableHandle>(
      obj["stream"].asString(),
      obj["kilnMetadata"].asString(),
      static_cast<uint64_t>(obj["chunkBytes"].asInt()));
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
    velox::connector::ConnectorQueryCtx* /*connectorQueryCtx*/,
    velox::connector::CommitStrategy /*commitStrategy*/) {
  VELOX_USER_CHECK(
      inputType->size() == 1 && inputType->childAt(0)->isVarbinary(),
      "A stoke stream takes one VARBINARY column of batches, not {}",
      inputType->toString());
  return std::make_unique<StokeDataSink>(
      *handle->asChecked<StokeInsertTableHandle>());
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
          stream, std::move(kilnMetadata), chunkBytes(session)),
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
