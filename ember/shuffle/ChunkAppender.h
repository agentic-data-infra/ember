#pragma once

#include <cstring>
#include <memory>
#include <optional>
#include <vector>

#include <folly/coro/BlockingWait.h>
#include <folly/executors/GlobalExecutor.h>
#include <folly/futures/Future.h>

#include "kiln/client/BufferPool.h"
#include "kiln/client/Client.h"

namespace ember::shuffle {

// Streams bytes into a chunk through two pooled buffers: fills one while Kiln
// appends the other, and takes one append per chunk at a time.
class ChunkAppender {
 public:
  ChunkAppender(
      std::shared_ptr<kiln::client::BufferPool> pool,
      kiln::client::ChunkWriter& chunk)
      : pool_{std::move(pool)},
        chunk_{chunk},
        buffers_{folly::coro::blockingWait(pool_->co_acquire(2))} {}

  // The append in flight uses its buffer until it is done. A failed one drops
  // it, and the pool replaces it.
  ~ChunkAppender() {
    try {
      awaitAppend();
    } catch (const std::exception&) {
    }
    pool_->release(std::move(buffers_));
  }

  void write(const void* data, size_t size) {
    auto* bytes = static_cast<const uint8_t*>(data);
    while (size > 0) {
      auto& buffer = buffers_[current_];
      const auto n = std::min(size, buffer.capacity() - buffer.size());
      std::memcpy(buffer.data() + buffer.size(), bytes, n);
      buffer.setSize(buffer.size() + n);
      bytes += n;
      size -= n;
      if (buffer.size() == buffer.capacity()) {
        flush();
      }
    }
  }

  // Waits until all bytes are in the chunk.
  void finish() {
    flush();
    awaitAppend();
  }

 private:
  // Waits for the append in flight, of the other buffer, and empties that
  // buffer for reuse.
  void awaitAppend() {
    if (append_.has_value()) {
      auto future = std::move(*append_);
      append_.reset();
      auto& other = buffers_[current_ ^ 1];
      other = std::move(future).get();
      other.setSize(0);
    }
  }

  // Starts appending the current buffer without waiting and goes on with the
  // other.
  void flush() {
    if (buffers_[current_].size() > 0) {
      awaitAppend();
      append_ = folly::coro::co_withExecutor(
                    folly::getGlobalCPUExecutor(),
                    chunk_.co_append(std::move(buffers_[current_])))
                    .start();
      current_ ^= 1;
    }
  }

  const std::shared_ptr<kiln::client::BufferPool> pool_;
  kiln::client::ChunkWriter& chunk_;
  std::vector<kiln::Buffer> buffers_;
  size_t current_{0};
  std::optional<folly::SemiFuture<kiln::Buffer>> append_;
};

} // namespace ember::shuffle
