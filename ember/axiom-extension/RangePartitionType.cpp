#include "ember/axiom-extension/RangePartitionType.h"

#include <fmt/format.h>

#include "ember/shuffle/RangePartitionFunction.h"
#include "velox/common/base/Exceptions.h"

namespace ember::axiom_extension {

namespace connector = facebook::axiom::connector;
namespace optimizer = facebook::axiom::optimizer;
namespace velox = facebook::velox;

std::shared_ptr<connector::PartitionType> RangePartitionType::copartition(
    const connector::PartitionType& /*other*/) const {
  return nullptr;
}

std::shared_ptr<connector::PartitionType> RangePartitionType::scaleDown(
    int32_t maxPartitions) const {
  VELOX_FAIL(
      "Range partitioning cannot scale down to {} partitions", maxPartitions);
}

velox::core::PartitionFunctionSpecPtr RangePartitionType::makeSpec(
    const std::vector<velox::column_index_t>& channels,
    const std::vector<velox::VectorPtr>& /*constants*/,
    bool isLocal) const {
  VELOX_CHECK(!isLocal, "Range partitioning is across tasks only");
  VELOX_CHECK_EQ(channels.size(), 1);
  return std::make_shared<shuffle::RangePartitionFunctionSpec>(
      channels[0], ascending_, nullsFirst_);
}

std::string RangePartitionType::toString() const {
  return fmt::format("range({})", numPartitions_);
}

optimizer::MultiFragmentPlan::SortPartitioner rangeSortPartitioner(
    int32_t numPartitions) {
  return [numPartitions](
             const velox::Type& keyType,
             bool ascending,
             bool nullsFirst,
             int32_t /*numWorkers*/)
             -> std::shared_ptr<const connector::PartitionType> {
    if (keyType.kind() != velox::TypeKind::VARCHAR &&
        keyType.kind() != velox::TypeKind::VARBINARY) {
      return nullptr;
    }
    return std::make_shared<RangePartitionType>(
        numPartitions, ascending, nullsFirst);
  };
}

} // namespace ember::axiom_extension
