// Range partitioning on sampled split points, for a global sort.
//
// The key is one VARCHAR or VARBINARY column. Partition p holds the keys up to
// split point p, in byte order, the order the sort uses, so every key of
// partition p sorts before every key of partition p + 1 (after, for a
// descending sort), and equal keys share a partition. Nulls go to the first or
// last partition, as the sort places them.
//
// The plan has no split points: the driver samples them when the stage runs,
// and fills them in with withBounds(). There may be fewer than
// 'numPartitions' - 1, when the sample has few distinct keys; later partitions
// stay empty.

#pragma once

#include "velox/core/PlanNode.h"

namespace ember::shuffle {

class RangePartitionFunction : public facebook::velox::core::PartitionFunction {
 public:
  RangePartitionFunction(
      int32_t numPartitions,
      facebook::velox::column_index_t keyChannel,
      bool ascending,
      bool nullsFirst,
      std::vector<std::string> bounds);

  std::optional<uint32_t> partition(
      const facebook::velox::RowVector& input,
      std::vector<uint32_t>& partitions) override;

 private:
  const int32_t numPartitions_;
  const facebook::velox::column_index_t keyChannel_;
  const bool ascending_;
  const bool nullsFirst_;
  const std::vector<std::string> bounds_;
};

class RangePartitionFunctionSpec
    : public facebook::velox::core::PartitionFunctionSpec {
 public:
  RangePartitionFunctionSpec(
      facebook::velox::column_index_t keyChannel,
      bool ascending,
      bool nullsFirst,
      std::vector<std::string> bounds = {})
      : keyChannel_{keyChannel},
        ascending_{ascending},
        nullsFirst_{nullsFirst},
        bounds_{std::move(bounds)} {}

  /// This spec with split points 'bounds', in ascending key order.
  std::shared_ptr<const RangePartitionFunctionSpec> withBounds(
      std::vector<std::string> bounds) const;

  std::unique_ptr<facebook::velox::core::PartitionFunction> create(
      int numPartitions,
      bool localExchange) const override;

  std::string toString() const override;

  folly::dynamic serialize() const override;

  static facebook::velox::core::PartitionFunctionSpecPtr deserialize(
      const folly::dynamic& obj,
      void* context);

 private:
  const facebook::velox::column_index_t keyChannel_;
  const bool ascending_;
  const bool nullsFirst_;
  const std::vector<std::string> bounds_;
};

} // namespace ember::shuffle
