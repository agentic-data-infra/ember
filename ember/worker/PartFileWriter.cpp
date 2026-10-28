#include "ember/worker/PartFileWriter.h"

#include "ember/GensortFileHandler.h"
#include "velox/common/file/File.h"
#include "velox/common/file/FileSystems.h"
#include "velox/vector/DecodedVector.h"

namespace ember {

namespace velox = facebook::velox;

namespace {

// Hidden from directory reads, which skip names starting with '.'.
std::string tempPath(
    const std::string& directory,
    int32_t partition,
    const std::string& taskId) {
  auto name = taskId;
  for (auto& c : name) {
    if (c == '/' || c == ':') {
      c = '_';
    }
  }
  return fmt::format("{}/.part-{:05d}.{}", directory, partition, name);
}

} // namespace

PartFileWriter::PartFileWriter(
    OutputFiles files,
    int32_t partition,
    const std::string& taskId)
    : files_{std::move(files)},
      path_{partPath(files_.directory, partition)},
      tempPath_{tempPath(files_.directory, partition, taskId)} {
  VELOX_USER_CHECK_EQ(
      files_.format, kGensortSchema, "Unsupported output format");
  fileSystem_ = velox::filesystems::getFileSystem(files_.directory, nullptr);
  fileSystem_->mkdir(files_.directory);
  file_ = fileSystem_->openFileForWrite(tempPath_);
}

PartFileWriter::~PartFileWriter() {
  if (committed_) {
    return;
  }
  try {
    file_.reset();
    fileSystem_->remove(tempPath_);
  } catch (const std::exception& e) {
    LOG(WARNING) << "Could not remove " << tempPath_ << ": " << e.what();
  }
}

std::string PartFileWriter::partPath(
    const std::string& directory,
    int32_t partition) {
  return fmt::format("{}/part-{:05d}", directory, partition);
}

void PartFileWriter::append(const velox::RowVectorPtr& rows) {
  const auto& type = *rows->rowType();
  VELOX_USER_CHECK(
      type.size() == 2 &&
          type.childAt(0)->kind() == velox::TypeKind::VARBINARY &&
          type.childAt(1)->kind() == velox::TypeKind::VARBINARY,
      "gensort output needs a key and a payload column, both VARBINARY: {}",
      type.toString());
  velox::DecodedVector keys(*rows->childAt(0));
  velox::DecodedVector payloads(*rows->childAt(1));
  buffer_.clear();
  buffer_.reserve(rows->size() * kGensortRecordSize);
  for (velox::vector_size_t i = 0; i < rows->size(); ++i) {
    VELOX_USER_CHECK(
        !keys.isNullAt(i) && !payloads.isNullAt(i),
        "gensort records cannot be null");
    const auto key = keys.valueAt<velox::StringView>(i);
    const auto payload = payloads.valueAt<velox::StringView>(i);
    VELOX_USER_CHECK_EQ(
        key.size() + payload.size(),
        kGensortRecordSize,
        "gensort records are {} bytes",
        kGensortRecordSize);
    buffer_.append(key.data(), key.size());
    buffer_.append(payload.data(), payload.size());
  }
  file_->append(buffer_);
  bytes_ += buffer_.size();
}

int64_t PartFileWriter::commit() {
  file_->close();
  fileSystem_->rename(tempPath_, path_, /*overwrite=*/true);
  committed_ = true;
  return bytes_;
}

} // namespace ember
