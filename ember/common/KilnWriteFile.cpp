#include "ember/KilnWriteFile.h"

#include <cstring>

#include <folly/coro/BlockingWait.h>

#include "ember/shuffle/KilnShuffle.h"
#include "kiln/client/BufferPool.h"
#include "kiln/client/Client.h"

namespace ember {

using folly::coro::blockingWait;

KilnWriteFile::KilnWriteFile(
    const std::string& metadata,
    kiln::BucketId bucket,
    const kiln::Placement& placement)
    : pool_{shuffle::kilnWritePool(metadata)} {
  writer_ =
      blockingWait(shuffle::kilnClient(metadata)->co_create(bucket, placement));
  // One buffer for the file's lifetime: a file never needs a second, so
  // files holding one never wait for each other.
  buffers_ = blockingWait(pool_->co_acquire(1));
}

KilnWriteFile::~KilnWriteFile() {
  pool_->release(std::move(buffers_));
  if (closed_) {
    return;
  }
  try {
    blockingWait(writer_->co_abort());
  } catch (const std::exception& e) {
    LOG(WARNING) << "Could not drop an unfinished Kiln chunk: " << e.what();
  }
}

void KilnWriteFile::append(std::string_view data) {
  VELOX_CHECK(!closed_, "Append to a closed Kiln file");
  while (!data.empty()) {
    auto& buffer = buffers_[0];
    const auto n = std::min(data.size(), buffer.capacity() - buffer.size());
    std::memcpy(buffer.data() + buffer.size(), data.data(), n);
    buffer.setSize(buffer.size() + n);
    data.remove_prefix(n);
    size_ += n;
    if (buffer.size() == buffer.capacity()) {
      appendBuffer();
    }
  }
}

void KilnWriteFile::appendBuffer() {
  buffers_ = blockingWait(writer_->co_append(std::move(buffers_)));
  buffers_[0].setSize(0);
}

void KilnWriteFile::close() {
  if (closed_) {
    return;
  }
  if (buffers_[0].size() > 0) {
    appendBuffer();
  }
  if (size_ == 0) {
    blockingWait(writer_->co_abort());
  } else {
    chunk_ = blockingWait(writer_->co_commit()).toString();
  }
  closed_ = true;
}

} // namespace ember
