// ShuffleRead: a leaf of a reduce stage's fragment. Reads this task's reduce
// partition of every map output of a shuffle. Its splits are shuffle block
// URLs (see ShuffleInterface.h), one per non-empty block; each is fetched
// through the BlockTransport.
//
// It reads like the exchange it replaces: an unordered ExchangeNode becomes a
// Velox Exchange operator, and a MergeExchangeNode (whose inputs are each
// sorted) becomes a MergeExchange, which merges the blocks in order.

#pragma once

#include "velox/core/PlanNode.h"
#include "velox/exec/ExchangeSource.h"
#include "velox/exec/Operator.h"

namespace ember::shuffle {

class ShuffleReadNode : public facebook::velox::core::PlanNode {
 public:
  /// 'exchange' is the ExchangeNode or MergeExchangeNode being replaced; this
  /// node takes its id and output type.
  ShuffleReadNode(
      int32_t shuffleId,
      std::shared_ptr<const facebook::velox::core::ExchangeNode> exchange);

  int32_t shuffleId() const {
    return shuffleId_;
  }

  bool isMerge() const;

  const std::shared_ptr<const facebook::velox::core::ExchangeNode>& exchange()
      const {
    return exchange_;
  }

  const facebook::velox::RowTypePtr& outputType() const override {
    return exchange_->outputType();
  }

  const std::vector<facebook::velox::core::PlanNodePtr>& sources()
      const override;

  bool requiresSplits() const override {
    return true;
  }

  /// A MergeExchange creates one client per input itself.
  bool requiresExchangeClient() const override {
    return !isMerge();
  }

  std::string_view name() const override {
    return "ShuffleRead";
  }

  folly::dynamic serialize() const override;

  static facebook::velox::core::PlanNodePtr create(
      const folly::dynamic& obj,
      void* context);

 private:
  void addDetails(std::stringstream& stream) const override;

  const int32_t shuffleId_;
  const std::shared_ptr<const facebook::velox::core::ExchangeNode> exchange_;
};

class ShuffleReadTranslator
    : public facebook::velox::exec::Operator::PlanNodeTranslator {
 public:
  std::unique_ptr<facebook::velox::exec::Operator> toOperator(
      facebook::velox::exec::DriverCtx* ctx,
      int32_t id,
      const facebook::velox::core::PlanNodePtr& node) override;

  std::unique_ptr<facebook::velox::exec::Operator> toOperator(
      facebook::velox::exec::DriverCtx* ctx,
      int32_t id,
      const facebook::velox::core::PlanNodePtr& node,
      std::shared_ptr<facebook::velox::exec::InMemoryExchangeClient>
          exchangeClient) override;

  std::optional<uint32_t> maxDrivers(
      const facebook::velox::core::PlanNodePtr& node) override;
};

/// Velox ExchangeSource factory for shuffle block URLs.
std::shared_ptr<facebook::velox::exec::ExchangeSource> createShuffleBlockSource(
    const std::string& url,
    int destination,
    std::shared_ptr<facebook::velox::exec::ExchangeQueue> queue,
    facebook::velox::memory::MemoryPool* pool);

} // namespace ember::shuffle
