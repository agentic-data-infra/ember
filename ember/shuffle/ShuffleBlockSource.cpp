#include "ember/shuffle/ShuffleBlockSource.h"

#include <folly/coro/BlockingWait.h>
#include <folly/coro/Invoke.h>

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

  int64_t nextLength() const override {
    return std::min(end_ - offset_, kBlockPieceBytes);
  }

  folly::coro::Task<std::unique_ptr<folly::IOBuf>> co_next() override {
    VELOX_CHECK(!atEnd());
    const auto length = nextLength();
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
    std::shared_ptr<ShuffleRuntime> runtime) {
  if (isKilnLocation(parseBlockUrl(url).first)) {
    return std::make_unique<KilnBlockReader>(url);
  }
  return std::make_unique<TransportBlockReader>(url, std::move(runtime));
}

// Reads one shuffle block as a stream: each request reads pieces until it has
// whole pages, about as many bytes as Velox
// asks for, and keeps the next piece read ahead. Velox asks for more as its
// queue drains, so a block holds only a few read buffers at a time, even
// in a merge, which holds a page of every source until it has used it up.
class ShuffleBlockSource : public exec::ExchangeSource {
 public:
  ShuffleBlockSource(
      const std::string& url,
      int destination,
      std::shared_ptr<exec::ExchangeQueue> queue,
      velox::memory::MemoryPool* pool)
      : ExchangeSource(url, destination, std::move(queue), pool),
        url_{url},
        runtime_{ShuffleRuntime::current()},
        reader_{createBlockReader(url, runtime_)} {}

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

  // The next piece's size, without waiting for it: starts reading it.
  folly::SemiFuture<Response> requestDataSizes(
      std::chrono::microseconds maxWait) override {
    if (reader_->atEnd() && pending_.empty()) {
      return request(0, maxWait);
    }
    reader_->prefetch();
    std::lock_guard<std::mutex> l(queue_->mutex());
    requestPending_ = false;
    return folly::makeSemiFuture(Response{0, false, remainingBytes()});
  }

  void close() override {}

  folly::F14FastMap<std::string, int64_t> stats() const override {
    return {
        {"shuffleBlockBytes", bytes_},
        {"kilnCopiedBytes", reader_->copiedBytes()}};
  }

 private:
  folly::coro::Task<Response> co_request(uint32_t maxBytes) {
    std::vector<std::unique_ptr<folly::IOBuf>> pages;
    int64_t bytes = 0;
    while (!reader_->atEnd() && (pages.empty() || bytes < maxBytes)) {
      auto piece = co_await folly::coro::co_awaitTry(reader_->co_next());
      if (piece.hasException()) {
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
      if (reader_->atEnd()) {
        queue_->enqueueLocked(nullptr, promises);
        atEnd_ = true;
      }
      remaining = remainingBytes();
    }
    for (auto& promise : promises) {
      promise.setValue();
    }
    co_return Response{bytes, reader_->atEnd(), std::move(remaining)};
  }

  // What Velox may ask for next: one piece, so that it asks again for each.
  std::vector<int64_t> remainingBytes() const {
    if (reader_->atEnd()) {
      return {};
    }
    return {reader_->nextLength()};
  }

  const std::string url_;
  const std::shared_ptr<ShuffleRuntime> runtime_;
  const std::unique_ptr<BlockReader> reader_;
  // Bytes read but not yet whole pages.
  folly::IOBufQueue pending_{folly::IOBufQueue::cacheChainLength()};
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

BlockPageReader::BlockPageReader(const std::string& url)
    : url_{url},
      runtime_{ShuffleRuntime::current()},
      reader_{createBlockReader(url, runtime_)} {}

BlockPageReader::~BlockPageReader() = default;

std::vector<std::unique_ptr<folly::IOBuf>> BlockPageReader::nextPages() {
  try {
    while (!reader_->atEnd()) {
      pending_.append(folly::coro::blockingWait(reader_->co_next()));
      auto pages = takeFrames(pending_);
      if (!pages.empty()) {
        return pages;
      }
    }
  } catch (const std::exception& e) {
    runtime_->failFetch(url_, e.what());
  }
  VELOX_CHECK(pending_.empty(), "{} ends in a cut page", url_);
  return {};
}

} // namespace ember::shuffle
