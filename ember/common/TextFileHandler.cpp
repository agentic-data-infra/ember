#include "ember/TextFileHandler.h"

#include <mutex>

#include "axiom/connectors/file/core/FileDataSources.h"
#include "ember/FileSerde.h"
#include "velox/common/file/FileSystems.h"
#include "velox/vector/FlatVector.h"

namespace ember {

namespace file = facebook::axiom::connector::file;
namespace velox = facebook::velox;

namespace {

// Bytes read from the file per pread.
constexpr uint64_t kReadSize = 4 << 20;

class TextDataSource : public file::StreamingDataSource {
 public:
  TextDataSource(
      const velox::RowTypePtr& outputType,
      const velox::connector::ColumnHandleMap& columnHandles,
      velox::memory::MemoryPool* pool)
      : StreamingDataSource(outputType, pool) {
    validateOutputColumns(outputType_, columnHandles);
    VELOX_CHECK_LE(outputType_->size(), 1);
  }

 protected:
  void onSplit(const std::string& filePath) override {
    file_ = velox::filesystems::getFileSystem(filePath, nullptr)
                ->openFileForRead(filePath);
    fileSize_ = file_->size();
    offset_ = 0;
    end_ = fileSize_;
    buffer_.clear();
    bufferPos_ = 0;

    // A range owns the lines that start in it: skip the line that starts
    // before it, and finish the last line past its end.
    if (auto range = std::dynamic_pointer_cast<FileRangeSplit>(split_)) {
      if (range->length >= 0) {
        end_ = std::min<uint64_t>(fileSize_, range->start + range->length);
      }
      if (range->start > 0) {
        offset_ = std::min<uint64_t>(fileSize_, range->start - 1);
        const auto end = end_;
        end_ = fileSize_;
        nextLine();
        end_ = end;
      }
    }
  }

  std::optional<velox::RowVectorPtr> readBatch(uint64_t size) override {
    std::string data;
    // Copy lines into 'data' first: the buffer moves on refill.
    std::vector<std::pair<size_t, size_t>> ranges;
    while (ranges.size() < size) {
      auto line = nextLine();
      if (!line.has_value()) {
        break;
      }
      ranges.emplace_back(data.size(), line->size());
      data.append(*line);
    }
    if (ranges.empty()) {
      return nullptr;
    }

    const auto numRows = static_cast<velox::vector_size_t>(ranges.size());
    std::vector<velox::VectorPtr> children;
    if (outputType_->size() == 1) {
      auto values = velox::BaseVector::create<velox::FlatVector<velox::StringView>>(
          velox::VARCHAR(), numRows, pool_);
      auto stringBuffer = velox::AlignedBuffer::allocate<char>(data.size(), pool_);
      auto* chars = stringBuffer->asMutable<char>();
      std::memcpy(chars, data.data(), data.size());
      for (velox::vector_size_t i = 0; i < numRows; ++i) {
        values->setNoCopy(
            i, velox::StringView(chars + ranges[i].first, ranges[i].second));
      }
      values->addStringBuffer(std::move(stringBuffer));
      children.push_back(std::move(values));
    }
    return std::make_shared<velox::RowVector>(
        pool_, outputType_, nullptr, numRows, std::move(children));
  }

 private:
  // Returns the next line without its terminator, or nullopt at the end of
  // the range.
  // The view is valid until the next call.
  std::optional<std::string_view> nextLine() {
    if (offset_ - buffer_.size() + bufferPos_ >= end_) {
      return std::nullopt;
    }
    while (true) {
      auto newline = buffer_.find('\n', bufferPos_);
      if (newline != std::string::npos) {
        return takeLine(newline, newline + 1);
      }
      if (offset_ >= fileSize_) {
        if (bufferPos_ < buffer_.size()) {
          return takeLine(buffer_.size(), buffer_.size());
        }
        return std::nullopt;
      }
      buffer_.erase(0, bufferPos_);
      bufferPos_ = 0;
      const auto length = std::min(kReadSize, fileSize_ - offset_);
      buffer_.append(file_->pread(offset_, length));
      offset_ += length;
    }
  }

  std::string_view takeLine(size_t end, size_t next) {
    std::string_view line(buffer_.data() + bufferPos_, end - bufferPos_);
    if (!line.empty() && line.back() == '\r') {
      line.remove_suffix(1);
    }
    bufferPos_ = next;
    return line;
  }

  std::unique_ptr<velox::ReadFile> file_;
  uint64_t fileSize_{0};
  uint64_t offset_{0};
  // Lines that start at or after 'end_' belong to the next range.
  uint64_t end_{0};
  std::string buffer_;
  size_t bufferPos_{0};
};

} // namespace

velox::RowTypePtr TextFileHandler::resolve(
    const std::string& /*filePath*/,
    velox::memory::MemoryPool* /*pool*/) const {
  return velox::ROW({kTextColumn}, {velox::VARCHAR()});
}

std::unique_ptr<velox::connector::DataSource> TextFileHandler::createDataSource(
    const velox::RowTypePtr& outputType,
    const velox::RowTypePtr& /*fullSchema*/,
    const velox::connector::ColumnHandleMap& columnHandles,
    velox::memory::MemoryPool* pool) const {
  return std::make_unique<TextDataSource>(outputType, columnHandles, pool);
}

void registerTextFileHandler() {
  static std::once_flag once;
  std::call_once(once, [] {
    static TextFileHandler handler;
    file::registerHandler(kTextSchema, handler);
  });
}

} // namespace ember
