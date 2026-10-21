#include "ember/shuffle/ShuffleRead.h"

#include "ember/shuffle/FileShuffle.h"
#include "velox/exec/Exchange.h"
#include "velox/exec/ExchangeSource.h"
#include "velox/exec/Merge.h"
#include "velox/exec/SerializedPage.h"

namespace ember::shuffle {

namespace core = facebook::velox::core;
namespace exec = facebook::velox::exec;
namespace velox = facebook::velox;

ShuffleReadNode::ShuffleReadNode(
    int32_t shuffleId,
    std::shared_ptr<const core::ExchangeNode> exchange)
    : PlanNode(exchange->id()),
      shuffleId_{shuffleId},
      exchange_{std::move(exchange)} {}

bool ShuffleReadNode::isMerge() const {
  return exchange_->is<core::MergeExchangeNode>();
}

const std::vector<core::PlanNodePtr>& ShuffleReadNode::sources() const {
  static const std::vector<core::PlanNodePtr> kNoSources;
  return kNoSources;
}

folly::dynamic ShuffleReadNode::serialize() const {
  auto obj = PlanNode::serialize();
  obj["shuffleId"] = shuffleId_;
  obj["exchange"] = exchange_->serialize();
  return obj;
}

core::PlanNodePtr ShuffleReadNode::create(
    const folly::dynamic& obj,
    void* context) {
  auto exchange = std::dynamic_pointer_cast<const core::ExchangeNode>(
      velox::ISerializable::deserialize<core::PlanNode>(
          obj["exchange"], context));
  VELOX_CHECK_NOT_NULL(exchange);
  return std::make_shared<const ShuffleReadNode>(
      obj["shuffleId"].asInt(), std::move(exchange));
}

void ShuffleReadNode::addDetails(std::stringstream& stream) const {
  stream << "shuffle " << shuffleId_;
  if (const auto* merge = exchange_->as<core::MergeExchangeNode>()) {
    stream << ", merge on ";
    for (size_t i = 0; i < merge->sortingKeys().size(); ++i) {
      stream << (i > 0 ? ", " : "") << merge->sortingKeys()[i]->toString()
             << " " << merge->sortingOrders()[i].toString();
    }
  }
}

std::unique_ptr<exec::Operator> ShuffleReadTranslator::toOperator(
    exec::DriverCtx* ctx,
    int32_t id,
    const core::PlanNodePtr& node) {
  auto read = std::dynamic_pointer_cast<const ShuffleReadNode>(node);
  if (read == nullptr || !read->isMerge()) {
    return nullptr;
  }
  return std::make_unique<exec::MergeExchange>(
      id,
      ctx,
      std::dynamic_pointer_cast<const core::MergeExchangeNode>(
          read->exchange()));
}

std::unique_ptr<exec::Operator> ShuffleReadTranslator::toOperator(
    exec::DriverCtx* ctx,
    int32_t id,
    const core::PlanNodePtr& node,
    std::shared_ptr<exec::InMemoryExchangeClient> exchangeClient) {
  auto read = std::dynamic_pointer_cast<const ShuffleReadNode>(node);
  if (read == nullptr) {
    return nullptr;
  }
  VELOX_CHECK(!read->isMerge());
  return std::make_unique<exec::Exchange>(
      id, ctx, read->exchange(), std::move(exchangeClient), "ShuffleRead");
}

std::optional<uint32_t> ShuffleReadTranslator::maxDrivers(
    const core::PlanNodePtr& node) {
  auto read = std::dynamic_pointer_cast<const ShuffleReadNode>(node);
  if (read != nullptr && read->isMerge()) {
    return 1;
  }
  return std::nullopt;
}

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
      : ExchangeSource(url, destination, std::move(queue), pool) {
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
    auto self = std::static_pointer_cast<ShuffleBlockSource>(shared_from_this());
    return BlockTransport::instance()
        ->fetchBlock(location_, block_)
        .deferValue([self](std::unique_ptr<folly::IOBuf> data) {
          return self->enqueue(std::move(data));
        });
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

  std::string location_;
  BlockId block_;
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

} // namespace ember::shuffle
