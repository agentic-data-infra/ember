#include "ember/shuffle/ShuffleBlockSource.h"

#include <array>

#include <folly/coro/BlockingWait.h>
#include <folly/coro/Invoke.h>
#include <folly/io/Cursor.h>
#include <folly/io/IOBufQueue.h>

#include "ember/shuffle/FileShuffle.h"
#include "ember/shuffle/KilnShuffle.h"
#include "velox/exec/ExchangeSource.h"
#include "velox/exec/SerializedPage.h"

namespace ember::shuffle {

namespace exec = facebook::velox::exec;
namespace velox = facebook::velox;

namespace {

// Reads a block in an executor's files through the BlockTransport.
class TransportBlockReader : public BlockReader {
 public:
  TransportBlockReader(
      const std::string& url,
      std::shared_ptr<ShuffleRuntime> runtime)
      : runtime_{std::move(runtime)} {
    std::tie(location_, block_) = parseBlockUrl(url);
    const auto ranges = blockUrlRanges(url);
    VELOX_CHECK_EQ(ranges.size(), 1, "Malformed shuffle block: {}", url);
    offset_ = ranges[0].first;
    end_ = offset_ + ranges[0].second;
  }

  bool atEnd() const override {
    return offset_ == end_;
  }

  int64_t nextLength(int64_t length) const override {
    return std::min({length, end_ - offset_, kBlockPieceBytes});
  }

  folly::coro::Task<std::unique_ptr<folly::IOBuf>> co_next(
      int64_t length) override {
    VELOX_CHECK(!atEnd());
    length = nextLength(length);
    auto piece = co_await runtime_->transport()->co_fetchBlock(
        location_, block_, offset_, length);
    offset_ += length;
    co_return piece;
  }

 private:
  const std::shared_ptr<ShuffleRuntime> runtime_;
  std::string location_;
  BlockId block_;
  int64_t offset_{0};
  int64_t end_{0};
};

std::unique_ptr<BlockReader> createBlockReader(
    const std::string& url,
    std::shared_ptr<ShuffleRuntime> runtime,
    std::shared_ptr<velox::memory::MemoryPool> pool) {
  if (isKilnLocation(parseBlockUrl(url).first)) {
    return std::make_unique<KilnBlockReader>(url, std::move(pool));
  }
  return std::make_unique<TransportBlockReader>(url, std::move(runtime));
}

bool isMemoryError(const folly::exception_wrapper& error) {
  const auto* e = error.get_exception<velox::VeloxRuntimeError>();
  if (e == nullptr) {
    return false;
  }
  namespace code = velox::error_code;
  for (const auto* memory :
       {code::kMemCapExceeded.c_str(),
        code::kMemArbitrationFailure.c_str(),
        code::kMemAborted.c_str(),
        code::kMemArbitrationTimeout.c_str(),
        code::kMemAllocError.c_str()}) {
    if (e->errorCode() == memory) {
      return true;
    }
  }
  return false;
}

} // namespace

// The pages of one block, read so that each read ends where a page does: once
// it knows a page's length, a read takes the rest of the page and the length
// of the next. A block between reads then holds only those few bytes, not the
// buffer of a page that left long ago.
class BlockPages {
 public:
  BlockPages(
      const std::string& url,
      std::shared_ptr<velox::memory::MemoryPool> pool)
      : url_{url},
        runtime_{ShuffleRuntime::current()},
        reader_{createBlockReader(url, runtime_, std::move(pool))} {}

  bool atEnd() const {
    return reader_->atEnd() && pending_.empty();
  }

  // The bytes the next read takes.
  int64_t nextReadBytes() const {
    if (pending_.chainLength() < kFrameHeaderBytes) {
      return reader_->nextLength(kBlockPieceBytes);
    }
    const int64_t length =
        folly::io::Cursor(pending_.front()).readLE<uint32_t>();
    return reader_->nextLength(
        kFrameHeaderBytes + length + kFrameHeaderBytes -
        pending_.chainLength());
  }

  // Starts the next read.
  void prefetch() {
    if (!reader_->atEnd()) {
      reader_->prefetch(nextReadBytes());
    }
  }

  // Whole pages of about 'minBytes' or more, at least one unless at the end.
  // Reads to the end of a page and then starts the next read. Records a
  // failed fetch and throws if the block is gone.
  folly::coro::Task<std::vector<std::unique_ptr<folly::IOBuf>>> co_next(
      int64_t minBytes) {
    std::vector<std::unique_ptr<folly::IOBuf>> pages;
    int64_t bytes = 0;
    while (!reader_->atEnd() &&
           (pages.empty() || bytes < minBytes ||
            pending_.chainLength() > kFrameHeaderBytes)) {
      auto piece =
          co_await folly::coro::co_awaitTry(reader_->co_next(nextReadBytes()));
      if (piece.hasException()) {
        // Memory for a copy is the task's to find, not a lost block.
        if (isMemoryError(piece.exception())) {
          co_yield folly::coro::co_error(std::move(piece.exception()));
        }
        runtime_->failFetch(url_, piece.exception().what().toStdString());
      }
      pending_.append(std::move(piece).value());
      for (auto& page : takeFrames(pending_)) {
        bytes += page->computeChainDataLength();
        pages.push_back(std::move(page));
      }
    }
    VELOX_CHECK(
        !reader_->atEnd() || pending_.empty(), "{} ends in a cut page", url_);
    // The next page's length, out of the buffer of the page before it.
    if (!pending_.empty()) {
      std::array<uint8_t, kFrameHeaderBytes> header;
      const auto size = pending_.chainLength();
      VELOX_CHECK_LE(size, kFrameHeaderBytes);
      folly::io::Cursor(pending_.front()).pull(header.data(), size);
      pending_.move();
      pending_.append(folly::IOBuf::copyBuffer(header.data(), size));
    }
    prefetch();
    co_return pages;
  }

  int64_t copiedBytes() const {
    return reader_->copiedBytes();
  }

 private:
  // The bytes of a page's length, before the page (see takeFrames()).
  static constexpr int64_t kFrameHeaderBytes = sizeof(uint32_t);

  const std::string url_;
  const std::shared_ptr<ShuffleRuntime> runtime_;
  const std::unique_ptr<BlockReader> reader_;
  // Bytes read but not yet whole pages.
  folly::IOBufQueue pending_{folly::IOBufQueue::cacheChainLength()};
};

namespace {

// Reads one shuffle block as a stream: each request reads whole pages, about
// as many bytes as Velox asks for, and starts the next read. Velox asks for
// more as its queue drains, so a block holds only a few pages at a time,
// even in a merge, which holds a page of every source until it has used it
// up. Pieces that are copied go into the exchange's memory pool.
class ShuffleBlockSource : public exec::ExchangeSource {
 public:
  ShuffleBlockSource(
      const std::string& url,
      int destination,
      std::shared_ptr<exec::ExchangeQueue> queue,
      velox::memory::MemoryPool* pool)
      : ExchangeSource(url, destination, std::move(queue), pool),
        pages_{url, pool->shared_from_this()} {}

  bool shouldRequestLocked() override {
    if (atEnd_) {
      return false;
    }
    return !requestPending_.exchange(true);
  }

  folly::SemiFuture<Response> request(
      uint32_t maxBytes,
      std::chrono::microseconds /*maxWait*/) override {
    auto self = std::static_pointer_cast<ShuffleBlockSource>(shared_from_this());
    return folly::coro::co_invoke([self, maxBytes] {
             return self->co_request(maxBytes);
           }).semi();
  }

  // The next read's size, without waiting for it: starts it.
  folly::SemiFuture<Response> requestDataSizes(
      std::chrono::microseconds maxWait) override {
    if (pages_.atEnd()) {
      return request(0, maxWait);
    }
    pages_.prefetch();
    std::lock_guard<std::mutex> l(queue_->mutex());
    requestPending_ = false;
    return folly::makeSemiFuture(Response{0, false, remainingBytes()});
  }

  void close() override {}

  folly::F14FastMap<std::string, int64_t> stats() const override {
    return {
        {"shuffleBlockBytes", bytes_},
        {"kilnCopiedBytes", pages_.copiedBytes()}};
  }

 private:
  folly::coro::Task<Response> co_request(uint32_t maxBytes) {
    auto pages = co_await pages_.co_next(maxBytes);
    int64_t bytes = 0;
    for (const auto& page : pages) {
      bytes += page->computeChainDataLength();
    }
    std::vector<velox::ContinuePromise> promises;
    std::vector<int64_t> remaining;
    {
      std::lock_guard<std::mutex> l(queue_->mutex());
      requestPending_ = false;
      bytes_ += bytes;
      for (auto& page : pages) {
        queue_->enqueueLocked(
            std::make_unique<exec::PrestoSerializedPage>(std::move(page)),
            promises);
      }
      if (pages_.atEnd()) {
        queue_->enqueueLocked(nullptr, promises);
        atEnd_ = true;
      }
      remaining = remainingBytes();
    }
    for (auto& promise : promises) {
      promise.setValue();
    }
    co_return Response{bytes, pages_.atEnd(), std::move(remaining)};
  }

  // What Velox may ask for next: one read, so that it asks again for each.
  std::vector<int64_t> remainingBytes() const {
    if (pages_.atEnd()) {
      return {};
    }
    return {pages_.nextReadBytes()};
  }

  BlockPages pages_;
  int64_t bytes_{0};
};

} // namespace

std::shared_ptr<exec::ExchangeSource> createShuffleBlockSource(
    const std::string& url,
    int destination,
    std::shared_ptr<exec::ExchangeQueue> queue,
    velox::memory::MemoryPool* pool) {
  if (!isBlockUrl(url)) {
    return nullptr;
  }
  return std::make_shared<ShuffleBlockSource>(
      url, destination, std::move(queue), pool);
}

BlockPageReader::BlockPageReader(
    const std::string& url,
    std::shared_ptr<velox::memory::MemoryPool> pool)
    : pages_{std::make_unique<BlockPages>(url, std::move(pool))} {}

BlockPageReader::~BlockPageReader() = default;

std::vector<std::unique_ptr<folly::IOBuf>> BlockPageReader::nextPages() {
  if (pages_->atEnd()) {
    return {};
  }
  return folly::coro::blockingWait(pages_->co_next(0));
}

} // namespace ember::shuffle
