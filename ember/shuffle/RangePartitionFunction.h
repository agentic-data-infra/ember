// Range partitioning on fixed splits of the key space, for a global sort.
//
// The key is one VARCHAR or VARBINARY column. Its first 8 bytes, read as a
// big-endian number (shorter keys padded with zero bytes), fall into one of
// 'numPartitions' equal slices of the 64-bit range. Byte order is the order
// the sort uses, so every key of partition p sorts before every key of
// partition p + 1 (after, for a descending sort), and equal keys share a
// partition. Nulls go to the first or last partition, as the sort places
// them.
//
// The splits are not sampled: they balance only keys that are uniformly
// random bytes, such as sortbenchmark.org's gensort keys. Other keys stay
// correct but may crowd into a few partitions.

#pragma once

#include "velox/core/PlanNode.h"

namespace ember::shuffle {

class RangePartitionFunction : public facebook::velox::core::PartitionFunction {
 public:
  RangePartitionFunction(
      int32_t numPartitions,
      facebook::velox::column_index_t keyChannel,
      bool ascending,
      bool nullsFirst);

  std::optional<uint32_t> partition(
      const facebook::velox::RowVector& input,
      std::vector<uint32_t>& partitions) override;

 private:
  const int32_t numPartitions_;
  const facebook::velox::column_index_t keyChannel_;
  const bool ascending_;
  const bool nullsFirst_;
};

class RangePartitionFunctionSpec
    : public facebook::velox::core::PartitionFunctionSpec {
 public:
  RangePartitionFunctionSpec(
      facebook::velox::column_index_t keyChannel,
      bool ascending,
      bool nullsFirst)
      : keyChannel_{keyChannel},
        ascending_{ascending},
        nullsFirst_{nullsFirst} {}

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
};

} // namespace ember::shuffle
