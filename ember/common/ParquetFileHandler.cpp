#include "ember/ParquetFileHandler.h"

#include <mutex>

#include "axiom/connectors/file/core/FileDataSources.h"
#include "ember/FileSerde.h"
#include "velox/common/file/FileSystems.h"
#include "velox/dwio/common/BufferedInput.h"
#include "velox/dwio/parquet/reader/ParquetReader.h"

namespace ember {

namespace file = facebook::axiom::connector::file;
namespace velox = facebook::velox;

namespace {

// Reads the file columns named by 'outputType'. A FileRangeSplit reads the
// row groups that start in its byte range.
class ParquetDataSource : public file::StreamingDataSource {
 public:
  ParquetDataSource(
      const velox::RowTypePtr& outputType,
      const velox::connector::ColumnHandleMap& columnHandles,
      velox::memory::MemoryPool* pool)
      : StreamingDataSource(outputType, pool) {
    validateOutputColumns(outputType_, columnHandles);
  }

 protected:
  void onSplit(const std::string& filePath) override {
    auto readFile = velox::filesystems::getFileSystem(filePath, nullptr)
                        ->openFileForRead(filePath);
    reader_ = std::make_unique<velox::parquet::ParquetReader>(
        std::make_unique<velox::dwio::common::BufferedInput>(
            std::move(readFile), *pool_),
        velox::dwio::common::ReaderOptions(pool_));
    velox::dwio::common::RowReaderOptions options;
    options.select(
        std::make_shared<velox::dwio::common::ColumnSelector>(
            reader_->rowType(), outputType_->names()));
    auto scanSpec = std::make_shared<velox::common::ScanSpec>("");
    scanSpec->addAllChildFields(*outputType_);
    options.setScanSpec(std::move(scanSpec));
    if (auto range = std::dynamic_pointer_cast<FileRangeSplit>(split_);
        range != nullptr && range->length >= 0) {
      options.range(range->start, range->length);
    }
    rowReader_ = reader_->createRowReader(options);
    output_ = velox::BaseVector::create(outputType_, 1, pool_);
  }

  std::optional<velox::RowVectorPtr> readBatch(uint64_t size) override {
    if (rowReader_->next(size, output_) == 0) {
      return nullptr;
    }
    auto rows = std::dynamic_pointer_cast<velox::RowVector>(output_);
    for (auto i = 0; i < rows->childrenSize(); ++i) {
      velox::BaseVector::flattenVector(rows->childAt(i));
    }
    return rows;
  }

 private:
  std::unique_ptr<velox::parquet::ParquetReader> reader_;
  std::unique_ptr<velox::dwio::common::RowReader> rowReader_;
  velox::VectorPtr output_;
};

// Returns the rows of 'source', which reads file columns, with the columns
// at 'channels' in the order and under the names of 'outputType'.
class RenamingDataSource : public velox::connector::DataSource {
 public:
  RenamingDataSource(
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

 private:
  const velox::RowTypePtr outputType_;
  const std::vector<velox::column_index_t> channels_;
  const std::unique_ptr<velox::connector::DataSource> source_;
};

} // namespace

std::unique_ptr<velox::connector::DataSource>
ParquetFileHandler::createDataSource(
    const velox::RowTypePtr& outputType,
    const velox::RowTypePtr& /*fullSchema*/,
    const velox::connector::ColumnHandleMap& columnHandles,
    velox::memory::MemoryPool* pool) const {
  // Each file column once, even if several output columns read it.
  std::vector<std::string> names;
  std::vector<velox::TypePtr> types;
  velox::connector::ColumnHandleMap handles;
  std::vector<velox::column_index_t> channels;
  bool renamed = false;
  for (auto i = 0; i < outputType->size(); ++i) {
    const auto& outputName = outputType->nameOf(i);
    auto it = columnHandles.find(outputName);
    VELOX_CHECK(
        it != columnHandles.end(), "No handle for output column: {}", outputName);
    const auto& name = it->second->name();
    renamed |= name != outputName;
    auto [entry, inserted] = handles.emplace(name, it->second);
    if (inserted) {
      names.push_back(name);
      types.push_back(outputType->childAt(i));
    } else {
      renamed = true;
    }
    channels.push_back(
        std::find(names.begin(), names.end(), name) - names.begin());
  }
  if (!renamed) {
    return std::make_unique<ParquetDataSource>(outputType, columnHandles, pool);
  }
  return std::make_unique<RenamingDataSource>(
      outputType,
      std::move(channels),
      std::make_unique<ParquetDataSource>(
          velox::ROW(std::move(names), std::move(types)), handles, pool));
}

void registerParquetFileHandler() {
  static std::once_flag once;
  std::call_once(once, [] {
    static ParquetFileHandler handler;
    file::registerHandler("parquet", handler);
  });
}

} // namespace ember
