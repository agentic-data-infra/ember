#include "ember/CachedConnector.h"

#include "axiom/connectors/ConnectorMetadataRegistry.h"
#include "ember/CachedTable.h"
#include "ember/shuffle/BackgroundWork.h"
#include "ember/shuffle/ShuffleBlockSource.h"
#include "velox/common/serialization/Serializable.h"
#include "velox/exec/OperatorUtils.h"
#include "velox/exec/SerializedPage.h"

namespace ember {

namespace axiom_connector = facebook::axiom::connector;
namespace exec = facebook::velox::exec;
namespace velox = facebook::velox;

namespace {

// Returns the rows of the blocks of its splits, a Presto page at a time.
class CachedDataSource : public velox::connector::DataSource {
 public:
  CachedDataSource(
      velox::RowTypePtr outputType,
      const CachedTableHandle& table,
      const velox::connector::ColumnHandleMap& columnHandles,
      velox::memory::MemoryPool* pool)
      : outputType_{std::move(outputType)},
        schema_{table.schema()},
        serde_{velox::getNamedVectorSerde("Presto")},
        pool_{pool},
        fetch_{shuffle::ShuffleRuntime::current()->ioExecutor()} {
    for (const auto& name : outputType_->names()) {
      channels_.push_back(schema_->getChildIdx(columnHandles.at(name)->name()));
    }
  }

  void addSplit(std::shared_ptr<velox::connector::ConnectorSplit> split)
      override {
    const auto* block = split->asChecked<CachedBlockSplit>();
    reader_ = std::make_unique<shuffle::BlockPageReader>(block->url);
    serdeOptions_ = exec::getVectorSerdeOptions(
        velox::common::stringToCompressionKind(block->compressionKind),
        "Presto",
        std::nullopt,
        /*minCompressionPageSizeBytes=*/0);
  }

  std::optional<velox::RowVectorPtr> next(
      uint64_t /*size*/,
      velox::ContinueFuture& future) override {
    while (true) {
      if (fetch_.blocked(&future)) {
        return std::nullopt;
      }
      if (stream_ != nullptr && !stream_->atEnd()) {
        velox::RowVectorPtr rows;
        serde_->deserialize(
            stream_.get(), pool_, schema_, &rows, 0, serdeOptions_.get());
        if (rows->size() > 0) {
          completedRows_ += rows->size();
          return project(rows);
        }
        continue;
      }
      stream_ = nullptr;
      if (nextPage_ < pages_.size()) {
        page_ = std::make_unique<exec::PrestoSerializedPage>(
            std::move(pages_[nextPage_++]));
        completedBytes_ += page_->size();
        stream_ = page_->prepareStreamForDeserialize();
      } else if (reader_ != nullptr) {
        fetch_.start([this] {
          pages_ = reader_->nextPages();
          nextPage_ = 0;
          if (pages_.empty()) {
            reader_ = nullptr;
          }
        });
      } else {
        return nullptr;
      }
    }
  }

  void addDynamicFilter(
      velox::column_index_t /*outputChannel*/,
      const std::shared_ptr<velox::common::Filter>& /*filter*/) override {
    VELOX_UNSUPPORTED("Cached tables take no filters");
  }

  uint64_t getCompletedBytes() override {
    return completedBytes_;
  }

  uint64_t getCompletedRows() override {
    return completedRows_;
  }

 private:
  velox::RowVectorPtr project(const velox::RowVectorPtr& rows) const {
    std::vector<velox::VectorPtr> children;
    children.reserve(channels_.size());
    for (auto channel : channels_) {
      children.push_back(rows->childAt(channel));
    }
    return std::make_shared<velox::RowVector>(
        pool_, outputType_, nullptr, rows->size(), std::move(children));
  }

  const velox::RowTypePtr outputType_;
  const velox::RowTypePtr schema_;
  velox::VectorSerde* const serde_;
  velox::memory::MemoryPool* const pool_;
  std::vector<velox::column_index_t> channels_;
  std::unique_ptr<velox::VectorSerde::Options> serdeOptions_;
  std::unique_ptr<shuffle::BlockPageReader> reader_;
  std::vector<std::unique_ptr<folly::IOBuf>> pages_;
  size_t nextPage_{0};
  std::unique_ptr<exec::PrestoSerializedPage> page_;
  std::unique_ptr<velox::ByteInputStream> stream_;
  uint64_t completedBytes_{0};
  uint64_t completedRows_{0};
  // Fetches pages off the driver thread. Last, so that it waits for them
  // before the rest goes.
  shuffle::BackgroundWork fetch_;
};

class CachedConnector : public velox::connector::Connector {
 public:
  CachedConnector() : Connector(std::string(kCachedConnectorId)) {}

  std::unique_ptr<velox::connector::DataSource> createDataSource(
      const velox::RowTypePtr& outputType,
      const velox::connector::ConnectorTableHandlePtr& tableHandle,
      const velox::connector::ColumnHandleMap& columnHandles,
      velox::connector::ConnectorQueryCtx* connectorQueryCtx) override {
    return std::make_unique<CachedDataSource>(
        outputType,
        *tableHandle->asChecked<CachedTableHandle>(),
        columnHandles,
        connectorQueryCtx->memoryPool());
  }

  std::unique_ptr<velox::connector::DataSink> createDataSink(
      velox::RowTypePtr /*inputType*/,
      velox::connector::ConnectorInsertTableHandlePtr /*handle*/,
      velox::connector::ConnectorQueryCtx* /*connectorQueryCtx*/,
      velox::connector::CommitStrategy /*commitStrategy*/) override {
    VELOX_UNSUPPORTED("Cached tables are not written");
  }
};

class CachedTableLayout : public axiom_connector::TableLayout {
 public:
  CachedTableLayout(
      const axiom_connector::Table* table,
      velox::connector::Connector* connector,
      std::vector<const axiom_connector::Column*> columns)
      : TableLayout(
            std::string(kCachedConnectorId),
            table,
            connector,
            std::move(columns),
            /*partitionColumns=*/{},
            /*orderColumns=*/{},
            /*sortOrder=*/{},
            /*lookupKeys=*/{},
            /*supportsScan=*/true) {}

  velox::connector::ColumnHandlePtr createColumnHandle(
      const axiom_connector::ConnectorSessionPtr& /*session*/,
      const std::string& columnName,
      std::vector<velox::common::Subfield> /*subfields*/,
      std::optional<velox::TypePtr> /*castToType*/,
      axiom_connector::SubfieldMapping /*subfieldMapping*/) const override {
    VELOX_CHECK_NOT_NULL(
        findColumn(columnName), "Column not found: {}", columnName);
    return std::make_shared<CachedColumnHandle>(columnName);
  }

  // Takes no filters: they stay above the scan.
  velox::connector::ConnectorTableHandlePtr createTableHandle(
      const axiom_connector::ConnectorSessionPtr& /*session*/,
      std::vector<velox::connector::ColumnHandlePtr> /*columnHandles*/,
      velox::core::ExpressionEvaluator& /*evaluator*/,
      std::vector<velox::core::TypedExprPtr> filters,
      std::vector<int32_t>& rejectedFilterIndices,
      velox::RowTypePtr /*dataColumns*/,
      std::optional<axiom_connector::LookupKeys> /*lookupKeys*/)
      const override {
    rejectedFilterIndices.resize(filters.size());
    std::iota(rejectedFilterIndices.begin(), rejectedFilterIndices.end(), 0);
    return std::make_shared<CachedTableHandle>(
        table().name().table, table().type());
  }
};

class CachedConnectorTable : public axiom_connector::Table {
 public:
  CachedConnectorTable(
      facebook::axiom::SchemaTableName name,
      const velox::RowTypePtr& type,
      velox::connector::Connector* connector)
      : Table(std::move(name), makeColumns(type)),
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
  const CachedTableLayout layout_;
  const std::vector<const axiom_connector::TableLayout*> layouts_;
};

// Finds the tables in the CachedTable registry. The driver enumerates no
// splits for them, and they are not listed.
class CachedConnectorMetadata : public axiom_connector::ConnectorMetadata {
 public:
  explicit CachedConnectorMetadata(velox::connector::Connector* connector)
      : connector_{connector} {}

  axiom_connector::TablePtr findTable(
      const facebook::axiom::SchemaTableName& tableName) override {
    auto table = findCachedTable(tableName.table);
    VELOX_USER_CHECK_NOT_NULL(table, "No cached table {}", tableName.table);
    return std::make_shared<CachedConnectorTable>(
        tableName, table->schema, connector_);
  }

  axiom_connector::ConnectorSplitManager* splitManager() override {
    return nullptr;
  }

  std::vector<std::string> listSchemaNames(
      const axiom_connector::ConnectorSessionPtr& /*session*/) override {
    return {};
  }

  bool schemaExists(
      const axiom_connector::ConnectorSessionPtr& /*session*/,
      const std::string& schemaName) override {
    return schemaName == kCachedSchema;
  }

  std::vector<std::string> listTableNames(
      const axiom_connector::ConnectorSessionPtr& /*session*/,
      const std::string& /*schemaName*/) override {
    VELOX_UNSUPPORTED("Cached tables are not listed");
  }

 private:
  velox::connector::Connector* const connector_;
};

} // namespace

CachedTableHandle::CachedTableHandle(std::string id, velox::RowTypePtr schema)
    : ConnectorTableHandle(std::string(kCachedConnectorId)),
      id_{std::move(id)},
      schema_{std::move(schema)} {}

folly::dynamic CachedTableHandle::serialize() const {
  auto obj = serializeBase("CachedTableHandle");
  obj["id"] = id_;
  obj["schema"] = schema_->serialize();
  return obj;
}

std::shared_ptr<CachedTableHandle> CachedTableHandle::create(
    const folly::dynamic& obj,
    void* context) {
  return std::make_shared<CachedTableHandle>(
      obj["id"].asString(),
      velox::ISerializable::deserialize<velox::RowType>(
          obj["schema"], context));
}

folly::dynamic CachedColumnHandle::serialize() const {
  auto obj = serializeBase("CachedColumnHandle");
  obj["columnName"] = name_;
  return obj;
}

std::shared_ptr<CachedColumnHandle> CachedColumnHandle::create(
    const folly::dynamic& obj,
    void* /*context*/) {
  return std::make_shared<CachedColumnHandle>(obj["columnName"].asString());
}

folly::dynamic CachedBlockSplit::serialize() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["name"] = "CachedBlockSplit";
  obj["url"] = url;
  obj["compressionKind"] = compressionKind;
  return obj;
}

std::shared_ptr<CachedBlockSplit> CachedBlockSplit::create(
    const folly::dynamic& obj) {
  return std::make_shared<CachedBlockSplit>(
      obj["url"].asString(), obj["compressionKind"].asString());
}

void registerCachedConnector() {
  auto& registry = velox::DeserializationWithContextRegistryForSharedPtr();
  registry.Register("CachedTableHandle", CachedTableHandle::create);
  registry.Register("CachedColumnHandle", CachedColumnHandle::create);
  velox::DeserializationRegistryForSharedPtr().Register(
      "CachedBlockSplit", CachedBlockSplit::create);
  auto connector = std::make_shared<CachedConnector>();
  velox::connector::registerConnector(connector);
  axiom_connector::ConnectorMetadataRegistry::global().insert(
      connector->connectorId(),
      std::make_shared<CachedConnectorMetadata>(connector.get()));
}

} // namespace ember
