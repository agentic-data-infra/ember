#include "ember/GensortFileHandler.h"

#include <mutex>

#include "axiom/connectors/file/core/FileDataSources.h"
#include "ember/FileSerde.h"
#include "velox/common/file/FileSystems.h"
#include "velox/vector/FlatVector.h"

namespace ember {

namespace file = facebook::axiom::connector::file;
namespace velox = facebook::velox;

namespace {

class GensortDataSource : public file::StreamingDataSource {
 public:
  GensortDataSource(
      const velox::RowTypePtr& outputType,
      const velox::connector::ColumnHandleMap& columnHandles,
      velox::memory::MemoryPool* pool)
      : StreamingDataSource(outputType, pool) {
    validateOutputColumns(outputType_, columnHandles);
    // Output columns may be renamed: the handles name the file columns.
    for (const auto& name : outputType_->names()) {
      const auto& column = columnHandles.at(name)->name();
      VELOX_CHECK(
          column == kGensortKeyColumn || column == kGensortPayloadColumn,
          "Not a gensort column: {}",
          column);
      isKey_.push_back(column == kGensortKeyColumn);
    }
  }

 protected:
  void onSplit(const std::string& filePath) override {
    file_ = velox::filesystems::getFileSystem(filePath, nullptr)
                ->openFileForRead(filePath);
    const auto fileSize = static_cast<int64_t>(file_->size());
    VELOX_USER_CHECK_EQ(
        fileSize % kGensortRecordSize,
        0,
        "Not a gensort file of {}-byte records: {}",
        kGensortRecordSize,
        filePath);
    offset_ = 0;
    end_ = fileSize;
    // A range owns the records that start in it.
    if (auto range = std::dynamic_pointer_cast<FileRangeSplit>(split_)) {
      offset_ = (range->start + kGensortRecordSize - 1) / kGensortRecordSize *
          kGensortRecordSize;
      if (range->length >= 0) {
        end_ = std::min(fileSize, range->start + range->length);
      }
    }
  }

  std::optional<velox::RowVectorPtr> readBatch(uint64_t size) override {
    if (offset_ >= end_) {
      return nullptr;
    }
    const auto numRows = static_cast<velox::vector_size_t>(std::min<int64_t>(
        size,
        (end_ - offset_ + kGensortRecordSize - 1) / kGensortRecordSize));
    const auto bytes = numRows * kGensortRecordSize;
    auto buffer = velox::AlignedBuffer::allocate<char>(bytes, pool_);
    auto* records = buffer->asMutable<char>();
    file_->pread(offset_, bytes, records);
    offset_ += bytes;

    std::vector<velox::VectorPtr> children;
    for (const bool isKey : isKey_) {
      const auto start = isKey ? 0 : kGensortKeySize;
      const auto length =
          isKey ? kGensortKeySize : kGensortRecordSize - kGensortKeySize;
      auto values =
          velox::BaseVector::create<velox::FlatVector<velox::StringView>>(
              velox::VARBINARY(), numRows, pool_);
      for (velox::vector_size_t i = 0; i < numRows; ++i) {
        values->setNoCopy(
            i,
            velox::StringView(
                records + i * kGensortRecordSize + start, length));
      }
      values->addStringBuffer(buffer);
      children.push_back(std::move(values));
    }
    return std::make_shared<velox::RowVector>(
        pool_, outputType_, nullptr, numRows, std::move(children));
  }

 private:
  // Per output column: the key, else the payload.
  std::vector<bool> isKey_;
  std::unique_ptr<velox::ReadFile> file_;
  // The next record to read.
  int64_t offset_{0};
  // Records that start at or after 'end_' belong to the next range.
  int64_t end_{0};
};

} // namespace

velox::RowTypePtr GensortFileHandler::resolve(
    const std::string& /*filePath*/,
    velox::memory::MemoryPool* /*pool*/) const {
  return velox::ROW(
      {kGensortKeyColumn, kGensortPayloadColumn},
      {velox::VARBINARY(), velox::VARBINARY()});
}

std::unique_ptr<velox::connector::DataSource>
GensortFileHandler::createDataSource(
    const velox::RowTypePtr& outputType,
    const velox::RowTypePtr& /*fullSchema*/,
    const velox::connector::ColumnHandleMap& columnHandles,
    velox::memory::MemoryPool* pool) const {
  return std::make_unique<GensortDataSource>(outputType, columnHandles, pool);
}

void registerGensortFileHandler() {
  static std::once_flag once;
  std::call_once(once, [] {
    static GensortFileHandler handler;
    file::registerHandler(kGensortSchema, handler);
  });
}

} // namespace ember
