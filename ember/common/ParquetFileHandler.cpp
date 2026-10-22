#include "ember/ParquetFileHandler.h"

#include <mutex>

namespace ember {

namespace file = facebook::axiom::connector::file;
namespace velox = facebook::velox;

namespace {

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
    const velox::RowTypePtr& fullSchema,
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
    return file::ParquetFileHandler::createDataSource(
        outputType, fullSchema, columnHandles, pool);
  }
  return std::make_unique<RenamingDataSource>(
      outputType,
      std::move(channels),
      file::ParquetFileHandler::createDataSource(
          velox::ROW(std::move(names), std::move(types)),
          fullSchema,
          handles,
          pool));
}

void registerParquetFileHandler() {
  static std::once_flag once;
  std::call_once(once, [] {
    static ParquetFileHandler handler;
    file::registerHandler("parquet", handler);
  });
}

} // namespace ember
