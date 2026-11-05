// The partitioning Axiom plans for a global sort that feeds a table write. It
// sends each key range to one task, using shuffle::RangePartitionFunction.
// Equal keys share a partition, as Axiom expects of any partitioning.

#pragma once

#include "axiom/optimizer/MultiFragmentPlan.h"

namespace ember::axiom_extension {

class RangePartitionType : public facebook::axiom::connector::PartitionType {
 public:
  RangePartitionType(int32_t numPartitions, bool ascending, bool nullsFirst)
      : numPartitions_{numPartitions},
        ascending_{ascending},
        nullsFirst_{nullsFirst} {}

  // Ranges pair with no other partitioning.
  std::shared_ptr<facebook::axiom::connector::PartitionType> copartition(
      const facebook::axiom::connector::PartitionType& other) const override;

  // Axiom scales only the partitioning of a table layout, which this is not.
  std::shared_ptr<facebook::axiom::connector::PartitionType> scaleDown(
      int32_t maxPartitions) const override;

  facebook::velox::core::PartitionFunctionSpecPtr makeSpec(
      const std::vector<facebook::velox::column_index_t>& channels,
      const std::vector<facebook::velox::VectorPtr>& constants,
      bool isLocal) const override;

  int32_t numPartitions() const override {
    return numPartitions_;
  }

  int32_t numGroups() const override {
    return numPartitions_;
  }

  std::string toString() const override;

 private:
  const int32_t numPartitions_;
  const bool ascending_;
  const bool nullsFirst_;
};

// Returns a Options::sortPartitioner that plans 'numPartitions' ranges for the
// keys RangePartitionFunction can split, and keeps the merge for the others.
facebook::axiom::optimizer::MultiFragmentPlan::SortPartitioner
rangeSortPartitioner(int32_t numPartitions);

} // namespace ember::axiom_extension
