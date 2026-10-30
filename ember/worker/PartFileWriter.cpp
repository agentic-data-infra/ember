#include "ember/worker/PartFileWriter.h"

#include <folly/coro/BlockingWait.h>

#include "ember/GensortFileHandler.h"
#include "ember/KilnFiles.h"
#include "ember/shuffle/KilnShuffle.h"
#include "kiln/client/Client.h"
#include "velox/common/file/File.h"
#include "velox/common/file/FileSystems.h"
#include "velox/vector/DecodedVector.h"

namespace ember {

namespace velox = facebook::velox;
using folly::coro::blockingWait;

namespace {

// Bytes buffered before they are appended to a Kiln chunk.
constexpr size_t kKilnAppendBytes = 4 << 20;

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
    const std::string& taskId,
    const std::map<std::string, std::string>& config)
    : files_{std::move(files)},
      path_{partPath(files_.directory, partition)},
      tempPath_{tempPath(files_.directory, partition, taskId)} {
  VELOX_USER_CHECK_EQ(
      files_.format, kGensortSchema, "Unsupported output format");
  if (isKilnPath(files_.directory)) {
    kiln_ = shuffle::kilnClient(parseKilnPath(files_.directory).metadata);
    const auto placement = kilnFilesPlacement(config);
    chunkWriter_ =
        blockingWait(kiln_->co_create(files_.kilnBucket, placement));
    kilnBuffer_ = std::make_unique<kiln::Buffer>(
        kiln_->allocateBuffer(kKilnAppendBytes));
    return;
  }
  fileSystem_ = velox::filesystems::getFileSystem(files_.directory, nullptr);
  fileSystem_->mkdir(files_.directory);
  file_ = fileSystem_->openFileForWrite(tempPath_);
}

PartFileWriter::~PartFileWriter() {
  if (committed_) {
    return;
  }
  try {
    if (chunkWriter_ != nullptr) {
      blockingWait(chunkWriter_->co_abort());
      return;
    }
    file_.reset();
    fileSystem_->remove(tempPath_);
  } catch (const std::exception& e) {
    LOG(WARNING) << "Could not remove " << tempPath_ << ": " << e.what();
  }
}

std::string PartFileWriter::partPath(
    const std::string& directory,
    int32_t partition) {
  return fmt::format("{}/{}", directory, partName(partition));
}

std::string PartFileWriter::partName(int32_t partition) {
  return fmt::format("part-{:05d}", partition);
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
  if (chunkWriter_ != nullptr) {
    appendToChunk(buffer_);
  } else {
    file_->append(buffer_);
  }
  bytes_ += buffer_.size();
}

void PartFileWriter::appendToChunk(const std::string& data) {
  auto& buffer = *kilnBuffer_;
  for (size_t done = 0; done < data.size();) {
    const auto n =
        std::min(data.size() - done, buffer.capacity() - buffer.size());
    std::memcpy(buffer.data() + buffer.size(), data.data() + done, n);
    buffer.setSize(buffer.size() + n);
    done += n;
    if (buffer.size() == buffer.capacity()) {
      buffer = blockingWait(chunkWriter_->co_append(std::move(buffer)));
      buffer.setSize(0);
    }
  }
}

int64_t PartFileWriter::commit() {
  if (chunkWriter_ != nullptr) {
    if (kilnBuffer_->size() > 0) {
      *kilnBuffer_ =
          blockingWait(chunkWriter_->co_append(std::move(*kilnBuffer_)));
    }
    if (bytes_ == 0) {
      // No chunk for no rows: the driver leaves the part out.
      blockingWait(chunkWriter_->co_abort());
    } else {
      chunk_ = blockingWait(chunkWriter_->co_commit()).toString();
    }
    committed_ = true;
    return bytes_;
  }
  file_->close();
  fileSystem_->rename(tempPath_, path_, /*overwrite=*/true);
  committed_ = true;
  return bytes_;
}

} // namespace ember
