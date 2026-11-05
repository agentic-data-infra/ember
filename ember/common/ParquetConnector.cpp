#include "ember/ParquetConnector.h"

#include <mutex>

#include "axiom/connectors/ConnectorMetadataRegistry.h"
#include "axiom/connectors/file/FileHandler.h"
#include "axiom/connectors/file/parquet/ParquetFileHandler.h"
#include "axiom/connectors/hive/HiveConnectorMetadata.h"
#include "ember/Runtime.h"
#include "velox/common/config/Config.h"
#include "velox/connectors/hive/HiveConfig.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/dwio/parquet/RegisterParquetReader.h"

namespace ember {

namespace axiom_connector = facebook::axiom::connector;
namespace axiom_hive = facebook::axiom::connector::hive;
namespace file = facebook::axiom::connector::file;
namespace hive = facebook::velox::connector::hive;
namespace velox = facebook::velox;

namespace {

// Returns the rows of 'source', which reads each table column once, with the
// columns at 'channels' under the names of 'outputType'. Velox's Hive data
// source does not map one table column to two outputs.
class RepeatedColumnsDataSource : public velox::connector::DataSource {
 public:
  RepeatedColumnsDataSource(
      velox::RowTypePtr outputType,
      std::vector<velox::column_index_t> channels,
      std::unique_ptr<velox::connector::DataSource> source)
      : outputType_{std::move(outputType)},
        channels_{std::move(channels)},
        source_{std::move(source)} {}

  void addSplit(std::shared_ptr<velox::connector::ConnectorSplit> split)
      override {
    source_->addSplit(std::move(split));
  }

  std::optional<velox::RowVectorPtr> next(
      uint64_t size,
      velox::ContinueFuture& future) override {
    auto result = source_->next(size, future);
    if (!result.has_value() || *result == nullptr) {
      return result;
    }
    const auto& input = *result;
    std::vector<velox::VectorPtr> children;
    children.reserve(channels_.size());
    for (auto channel : channels_) {
      children.push_back(input->childAt(channel));
    }
    return std::make_shared<velox::RowVector>(
        input->pool(),
        outputType_,
        nullptr,
        input->size(),
        std::move(children));
  }

  const velox::common::SubfieldFilters* getFilters() const override {
    return source_->getFilters();
  }

  void addDynamicFilter(
      velox::column_index_t outputChannel,
      const std::shared_ptr<velox::common::Filter>& filter) override {
    source_->addDynamicFilter(channels_.at(outputChannel), filter);
  }

  uint64_t getCompletedBytes() override {
    return source_->getCompletedBytes();
  }

  uint64_t getCompletedRows() override {
    return source_->getCompletedRows();
  }

  std::unordered_map<std::string, velox::RuntimeMetric> getRuntimeStats()
      override {
    return source_->getRuntimeStats();
  }

  int64_t estimatedRowSize() override {
    return source_->estimatedRowSize();
  }

  void cancel() override {
    source_->cancel();
  }

 private:
  const velox::RowTypePtr outputType_;
  const std::vector<velox::column_index_t> channels_;
  const std::unique_ptr<velox::connector::DataSource> source_;
};

std::shared_ptr<const velox::config::ConfigBase> parquetConfig() {
  // Files are rewritten under the same path, so handles are not cached by it.
  return std::make_shared<velox::config::ConfigBase>(
      std::unordered_map<std::string, std::string>{
          {hive::HiveConfig::kEnableFileHandleCache, "false"}});
}

class ParquetConnector : public hive::HiveConnector {
 public:
  explicit ParquetConnector(const std::string& id)
      : HiveConnector(id, parquetConfig(), /*executor=*/nullptr) {}

  std::unique_ptr<velox::connector::DataSource> createDataSource(
      const velox::RowTypePtr& outputType,
      const velox::connector::ConnectorTableHandlePtr& tableHandle,
      const velox::connector::ColumnHandleMap& columnHandles,
      velox::connector::ConnectorQueryCtx* connectorQueryCtx) override {
    // Each table column once, even if several output columns read it.
    std::vector<std::string> names;
    std::vector<velox::TypePtr> types;
    velox::connector::ColumnHandleMap handles;
    std::vector<velox::column_index_t> channels;
    std::map<std::string, velox::column_index_t> channelOfColumn;
    for (auto i = 0; i < outputType->size(); ++i) {
      const auto& name = outputType->nameOf(i);
      const auto& handle = columnHandles.at(name);
      auto [it, inserted] =
          channelOfColumn.emplace(handle->name(), names.size());
      if (inserted) {
        names.push_back(name);
        types.push_back(outputType->childAt(i));
        handles.emplace(name, handle);
      }
      channels.push_back(it->second);
    }
    if (names.size() == channels.size()) {
      return HiveConnector::createDataSource(
          outputType, tableHandle, columnHandles, connectorQueryCtx);
    }
    return std::make_unique<RepeatedColumnsDataSource>(
        outputType,
        std::move(channels),
        HiveConnector::createDataSource(
            velox::ROW(std::move(names), std::move(types)),
            tableHandle,
            handles,
            connectorQueryCtx));
  }
};

class ParquetTable : public axiom_hive::HiveTable {
 public:
  ParquetTable(
      facebook::axiom::SchemaTableName name,
      velox::RowTypePtr type,
      velox::connector::Connector* connector)
      : HiveTable(
            std::move(name),
            std::move(type),
            /*bucketed=*/false,
            /*includeHiddenColumns=*/false,
            /*options=*/{}) {
    layout_ = std::make_unique<axiom_hive::HiveTableLayout>(
        kParquetSchema,
        this,
        connector,
        allColumns(),
        /*numPartitions=*/std::nullopt,
        /*partitionedByColumns=*/std::vector<const axiom_connector::Column*>{},
        /*sortedByColumns=*/std::vector<const axiom_connector::Column*>{},
        /*sortOrder=*/std::vector<axiom_connector::SortOrder>{},
        /*lookupKeys=*/std::vector<const axiom_connector::Column*>{},
        /*hivePartitionedByColumns=*/
        std::vector<const axiom_connector::Column*>{},
        velox::dwio::common::FileFormat::PARQUET);
    layouts_.push_back(layout_.get());
  }

  const std::vector<const axiom_connector::TableLayout*>& layouts()
      const override {
    return layouts_;
  }

  std::optional<uint64_t> numRows() const override {
    return std::nullopt;
  }

 private:
  std::unique_ptr<axiom_hive::HiveTableLayout> layout_;
  std::vector<const axiom_connector::TableLayout*> layouts_;
};

// One whole-file split per file; the driver's scheduler cuts them into ranges.
class ParquetSplitSource : public axiom_connector::SplitSource {
 public:
  ParquetSplitSource(std::string connectorId, std::vector<std::string> files)
      : connectorId_{std::move(connectorId)}, files_{std::move(files)} {}

  folly::coro::Task<axiom_connector::SplitBatch> co_getSplits(
      uint32_t maxSplitCount) override {
    axiom_connector::SplitBatch batch;
    while (next_ < files_.size() && batch.splits.size() < maxSplitCount) {
      batch.splits.emplace_back(
          hive::HiveConnectorSplitBuilder(files_[next_++])
              .connectorId(connectorId_)
              .fileFormat(velox::dwio::common::FileFormat::PARQUET)
              .build());
    }
    batch.noMoreSplits = next_ == files_.size();
    co_return batch;
  }

 private:
  const std::string connectorId_;
  const std::vector<std::string> files_;
  size_t next_{0};
};

class ParquetSplitManager : public axiom_connector::ConnectorSplitManager {
 public:
  folly::coro::Task<std::vector<axiom_connector::PartitionHandlePtr>>
  co_listPartitions(
      const axiom_connector::ConnectorSessionPtr& /*session*/,
      const velox::connector::ConnectorTableHandlePtr& /*tableHandle*/,
      velox::core::ExpressionEvaluator& /*evaluator*/) override {
    co_return std::vector<axiom_connector::PartitionHandlePtr>{
        std::make_shared<axiom_connector::PartitionHandle>()};
  }

  std::shared_ptr<axiom_connector::SplitSource> getSplitSource(
      const axiom_connector::ConnectorSessionPtr& /*session*/,
      const velox::connector::ConnectorTableHandlePtr& tableHandle,
      const std::vector<axiom_connector::PartitionHandlePtr>& /*partitions*/,
      const std::shared_ptr<axiom_connector::PartitionType>& /*partitionType*/,
      std::optional<double> samplePercentage) override {
    VELOX_USER_CHECK(
        !samplePercentage.has_value(),
        "SYSTEM sampling is not supported by this connector");
    return std::make_shared<ParquetSplitSource>(
        tableHandle->connectorId(),
        file::handler(kParquetSchema).listFiles(tableHandle->name()));
  }
};

class ParquetMetadata : public axiom_connector::ConnectorMetadata {
 public:
  ParquetMetadata(
      velox::connector::Connector* connector,
      std::shared_ptr<axiom_connector::ConnectorMetadata> files)
      : connector_{connector}, files_{std::move(files)} {}

  axiom_connector::TablePtr findTable(
      const facebook::axiom::SchemaTableName& tableName) override {
    return std::make_shared<ParquetTable>(
        tableName, files_->findTable(tableName)->type(), connector_);
  }

  std::vector<std::string> listSchemaNames(
      const axiom_connector::ConnectorSessionPtr& /*session*/) override {
    return {kParquetSchema};
  }

  bool schemaExists(
      const axiom_connector::ConnectorSessionPtr& /*session*/,
      const std::string& schemaName) override {
    return schemaName == kParquetSchema;
  }

  std::vector<std::string> listTableNames(
      const axiom_connector::ConnectorSessionPtr& /*session*/,
      const std::string& /*schemaName*/) override {
    return {};
  }

  axiom_connector::ConnectorSplitManager* splitManager() override {
    return &splitManager_;
  }

 private:
  velox::connector::Connector* const connector_;
  const std::shared_ptr<axiom_connector::ConnectorMetadata> files_;
  ParquetSplitManager splitManager_;
};

} // namespace

void registerParquetConnector() {
  static std::once_flag once;
  std::call_once(once, [] {
    file::registerParquetHandler();
    velox::parquet::registerParquetReaderFactory();
    velox::common::Filter::registerSerDe();
    hive::HiveConnector::registerSerDe();
    auto connector = std::make_shared<ParquetConnector>(kParquetConnectorId);
    velox::connector::registerConnector(connector);
    axiom_connector::ConnectorMetadataRegistry::global().insert(
        connector->connectorId(),
        std::make_shared<ParquetMetadata>(
            connector.get(),
            axiom_connector::ConnectorMetadataRegistry::get(kFileConnectorId)));
  });
}

} // namespace ember
