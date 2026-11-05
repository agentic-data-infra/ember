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

// Reads one shuffle block: fetches it whole through the BlockTransport and
// enqueues its pages.
class ShuffleBlockSource : public exec::ExchangeSource {
 public:
  ShuffleBlockSource(
      const std::string& url,
      int destination,
      std::shared_ptr<exec::ExchangeQueue> queue,
      velox::memory::MemoryPool* pool)
      : ExchangeSource(url, destination, std::move(queue), pool),
        url_{url},
        context_{ShuffleContext::of(*pool)} {
    std::tie(location_, block_) = parseBlockUrl(url);
  }

  bool shouldRequestLocked() override {
    if (atEnd_) {
      return false;
    }
    return !requestPending_.exchange(true);
  }

  folly::SemiFuture<Response> request(
      uint32_t /*maxBytes*/,
      std::chrono::microseconds /*maxWait*/) override {
    // Velox takes a SemiFuture; the fetch starts on the executor it puts
    // the future on.
    auto self = std::static_pointer_cast<ShuffleBlockSource>(shared_from_this());
    return folly::coro::co_invoke([self] { return self->co_request(); }).semi();
  }

  folly::SemiFuture<Response> requestDataSizes(
      std::chrono::microseconds maxWait) override {
    return request(0, maxWait);
  }

  void close() override {}

  folly::F14FastMap<std::string, int64_t> stats() const override {
    return {{"shuffleBlockBytes", bytes_}};
  }

 private:
  folly::coro::Task<Response> co_request() {
    auto data = co_await folly::coro::co_awaitTry(
        context_->transport->co_fetchBlock(location_, block_));
    if (data.hasException()) {
      context_->fetchFailures.fail(
          {location_, block_}, data.exception().what().toStdString());
    }
    co_return enqueue(std::move(data).value());
  }

  Response enqueue(std::unique_ptr<folly::IOBuf> data) {
    auto pages = splitFrames(std::move(data));
    std::vector<velox::ContinuePromise> promises;
    {
      std::lock_guard<std::mutex> l(queue_->mutex());
      requestPending_ = false;
      for (auto& page : pages) {
        bytes_ += page->computeChainDataLength();
        queue_->enqueueLocked(
            std::make_unique<exec::PrestoSerializedPage>(std::move(page)),
            promises);
      }
      queue_->enqueueLocked(nullptr, promises);
      atEnd_ = true;
    }
    for (auto& promise : promises) {
      promise.setValue();
    }
    return Response{bytes_, true, {}};
  }

  const std::string url_;
  const std::shared_ptr<ShuffleContext> context_;
  std::string location_;
  BlockId block_;
  int64_t bytes_{0};
};

// Reads one shuffle block from Kiln as a stream: each request reads pieces
// (KilnBlockReader) until it has whole pages, about as many bytes as Velox
// asks for, and keeps the next piece read ahead. Velox asks for more as its
// queue drains, so a block holds only a few read buffers at a time, even
// in a merge, which holds a page of every source until it has used it up.
class KilnBlockSource : public exec::ExchangeSource {
 public:
  KilnBlockSource(
      const std::string& url,
      int destination,
      std::shared_ptr<exec::ExchangeQueue> queue,
      velox::memory::MemoryPool* pool)
      : ExchangeSource(url, destination, std::move(queue), pool),
        url_{url},
        context_{ShuffleContext::of(*pool)},
        reader_{url} {}

  bool shouldRequestLocked() override {
    if (atEnd_) {
      return false;
    }
    return !requestPending_.exchange(true);
  }

  folly::SemiFuture<Response> request(
      uint32_t maxBytes,
      std::chrono::microseconds /*maxWait*/) override {
    auto self = std::static_pointer_cast<KilnBlockSource>(shared_from_this());
    return folly::coro::co_invoke([self, maxBytes] {
             return self->co_request(maxBytes);
           }).semi();
  }

  // The next piece's size, without waiting for it: starts reading it.
  folly::SemiFuture<Response> requestDataSizes(
      std::chrono::microseconds maxWait) override {
    if (reader_.atEnd() && pending_.empty()) {
      return request(0, maxWait);
    }
    reader_.prefetch();
    std::lock_guard<std::mutex> l(queue_->mutex());
    requestPending_ = false;
    return folly::makeSemiFuture(Response{0, false, remainingBytes()});
  }

  void close() override {}

  folly::F14FastMap<std::string, int64_t> stats() const override {
    return {
        {"shuffleBlockBytes", bytes_},
        {"kilnCopiedBytes", reader_.copiedBytes()}};
  }

 private:
  folly::coro::Task<Response> co_request(uint32_t maxBytes) {
    std::vector<std::unique_ptr<folly::IOBuf>> pages;
    int64_t bytes = 0;
    while (!reader_.atEnd() && (pages.empty() || bytes < maxBytes)) {
      auto piece = co_await folly::coro::co_awaitTry(reader_.co_next());
      if (piece.hasException()) {
        auto [location, block] = parseBlockUrl(url_);
        context_->fetchFailures.fail(
            {std::move(location), block},
            piece.exception().what().toStdString());
      }
      pending_.append(std::move(piece).value());
      for (auto& page : takeFrames(pending_)) {
        bytes += page->computeChainDataLength();
        pages.push_back(std::move(page));
      }
    }
    VELOX_CHECK(
        !reader_.atEnd() || pending_.empty(), "{} ends in a cut page", url_);
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
      if (reader_.atEnd()) {
        queue_->enqueueLocked(nullptr, promises);
        atEnd_ = true;
      }
      remaining = remainingBytes();
    }
    for (auto& promise : promises) {
      promise.setValue();
    }
    co_return Response{bytes, reader_.atEnd(), std::move(remaining)};
  }

  // What Velox may ask for next: one piece, so that it asks again for each.
  std::vector<int64_t> remainingBytes() const {
    if (reader_.atEnd()) {
      return {};
    }
    return {reader_.nextLength()};
  }

  const std::string url_;
  const std::shared_ptr<ShuffleContext> context_;
  KilnBlockReader reader_;
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
  if (isKilnLocation(parseBlockUrl(url).first)) {
    return std::make_shared<KilnBlockSource>(
        url, destination, std::move(queue), pool);
  }
  return std::make_shared<ShuffleBlockSource>(
      url, destination, std::move(queue), pool);
}

BlockPageReader::BlockPageReader(
    const std::string& url,
    const velox::memory::MemoryPool& pool)
    : url_{url}, context_{ShuffleContext::of(pool)} {
  std::tie(location_, block_) = parseBlockUrl(url);
  if (isKilnLocation(location_)) {
    kiln_ = std::make_unique<KilnBlockReader>(url);
  }
}

BlockPageReader::~BlockPageReader() = default;

std::vector<std::unique_ptr<folly::IOBuf>> BlockPageReader::nextPages() {
  try {
    if (kiln_ == nullptr) {
      if (fetched_) {
        return {};
      }
      fetched_ = true;
      return splitFrames(folly::coro::blockingWait(
          context_->transport->co_fetchBlock(location_, block_)));
    }
    while (!kiln_->atEnd()) {
      pending_.append(folly::coro::blockingWait(kiln_->co_next()));
      auto pages = takeFrames(pending_);
      if (!pages.empty()) {
        return pages;
      }
    }
  } catch (const std::exception& e) {
    context_->fetchFailures.fail({location_, block_}, e.what());
  }
  VELOX_CHECK(pending_.empty(), "{} ends in a cut page", url_);
  return {};
}

} // namespace ember::shuffle
