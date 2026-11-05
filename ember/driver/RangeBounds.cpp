#include "ember/driver/RangeBounds.h"

#include <algorithm>
#include <cmath>

#include "ember/shuffle/RangePartitionFunction.h"
#include "ember/shuffle/ShuffleWrite.h"
#include "velox/core/Expressions.h"
#include "velox/vector/DecodedVector.h"

namespace ember::driver {

namespace core = facebook::velox::core;
namespace velox = facebook::velox;

namespace {

// As Spark's RangePartitioner: sample points per range, at most so many in
// all, and three times as many as an even share from each task, as tasks may
// differ in size.
constexpr int64_t kSamplePointsPerRange = 100;
constexpr int64_t kMaxSamplePoints = 1'000'000;
constexpr int64_t kOversampling = 3;

constexpr const char* kKeyColumn = "key";
constexpr const char* kSampleColumn = "sample";

const shuffle::ShuffleWriteNode& rangeWrite(const Stage& stage) {
  const auto* write = stage.plan->as<shuffle::ShuffleWriteNode>();
  VELOX_CHECK_NOT_NULL(write);
  return *write;
}

const core::FieldAccessTypedExprPtr keyField(const Stage& stage) {
  const auto& keys = rangeWrite(stage).keys();
  VELOX_CHECK_EQ(keys.size(), 1);
  auto field =
      std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(keys[0]);
  VELOX_CHECK_NOT_NULL(field);
  return field;
}

velox::TypePtr sampleResultType() {
  return velox::ROW(
      {"processed_count", "sample"},
      {velox::BIGINT(), velox::ARRAY(velox::VARCHAR())});
}

struct WeightedKey {
  std::string key;
  double weight;
};

} // namespace

bool writesRanges(const Stage& stage) {
  const auto* write = stage.plan->as<shuffle::ShuffleWriteNode>();
  return write != nullptr && write->numPartitions() > 1 &&
      std::dynamic_pointer_cast<const shuffle::RangePartitionFunctionSpec>(
          write->partitionFunctionSpec()) != nullptr;
}

int32_t numRanges(const Stage& stage) {
  return rangeWrite(stage).numPartitions();
}

folly::dynamic sampleFragment(const Stage& stage, int32_t numTasks) {
  const auto& write = rangeWrite(stage);
  const auto points =
      std::min(kSamplePointsPerRange * numRanges(stage), kMaxSamplePoints);
  const auto taskPoints = static_cast<int32_t>(std::ceil(
      static_cast<double>(kOversampling * points) / std::max(numTasks, 1)));

  // reservoir_sample does not take VARBINARY. The cast keeps the bytes.
  core::TypedExprPtr key = keyField(stage);
  if (key->type()->isVarbinary()) {
    key = std::make_shared<core::CastTypedExpr>(
        velox::VARCHAR(), key, /*isTryCast=*/false);
  }
  auto keys = std::make_shared<core::ProjectNode>(
      write.id() + "_sampleKey",
      std::vector<std::string>{kKeyColumn},
      std::vector<core::TypedExprPtr>{std::move(key)},
      write.sources()[0]);

  const auto keyType = velox::VARCHAR();
  const auto arrayType = velox::ARRAY(keyType);
  std::vector<core::TypedExprPtr> inputs{
      std::make_shared<core::ConstantTypedExpr>(
          arrayType, velox::Variant::null(velox::TypeKind::ARRAY)),
      std::make_shared<core::ConstantTypedExpr>(
          velox::BIGINT(), velox::Variant(int64_t{0})),
      std::make_shared<core::FieldAccessTypedExpr>(keyType, kKeyColumn),
      std::make_shared<core::ConstantTypedExpr>(
          velox::INTEGER(), velox::Variant(taskPoints))};
  core::AggregationNode::Aggregate aggregate{
      .call = std::make_shared<core::CallTypedExpr>(
          sampleResultType(), inputs, "reservoir_sample"),
      .rawInputTypes = {arrayType, velox::BIGINT(), keyType, velox::INTEGER()}};

  auto node = std::make_shared<core::AggregationNode>(
      write.id(),
      core::AggregationNode::Step::kSingle,
      std::vector<core::FieldAccessTypedExprPtr>{},
      std::vector<core::FieldAccessTypedExprPtr>{},
      std::vector<std::string>{kSampleColumn},
      std::vector<core::AggregationNode::Aggregate>{std::move(aggregate)},
      /*ignoreNullKeys=*/false,
      /*noGroupsSpanBatches=*/false,
      keys);
  return node->serialize();
}

velox::RowTypePtr sampleRowType() {
  return velox::ROW({kSampleColumn}, {sampleResultType()});
}

std::vector<std::string> rangeBounds(
    const std::vector<velox::RowVectorPtr>& samples,
    int32_t numRanges) {
  std::vector<WeightedKey> keys;
  for (const auto& rows : samples) {
    const auto* results = rows->childAt(0)->asChecked<velox::RowVector>();
    velox::DecodedVector counts(*results->childAt(0));
    const auto* arrays = results->childAt(1)->asChecked<velox::ArrayVector>();
    velox::DecodedVector elements(*arrays->elements());
    for (velox::vector_size_t row = 0; row < rows->size(); ++row) {
      if (results->isNullAt(row) || arrays->isNullAt(row)) {
        continue;
      }
      const auto offset = arrays->offsetAt(row);
      const auto size = arrays->sizeAt(row);
      if (size == 0) {
        continue;
      }
      const auto weight =
          static_cast<double>(counts.valueAt<int64_t>(row)) / size;
      for (auto i = offset; i < offset + size; ++i) {
        if (!elements.isNullAt(i)) {
          keys.push_back(
              {std::string(elements.valueAt<velox::StringView>(i)), weight});
        }
      }
    }
  }
  std::sort(keys.begin(), keys.end(), [](const auto& a, const auto& b) {
    return a.key < b.key;
  });

  double total = 0;
  for (const auto& key : keys) {
    total += key.weight;
  }
  const auto step = total / numRanges;
  std::vector<std::string> bounds;
  double seen = 0;
  for (const auto& key : keys) {
    seen += key.weight;
    const auto target = step * (bounds.size() + 1);
    if (seen >= target && static_cast<int32_t>(bounds.size()) < numRanges - 1 &&
        (bounds.empty() || key.key > bounds.back())) {
      bounds.push_back(key.key);
    }
  }
  return bounds;
}

std::shared_ptr<const shuffle::ShuffleWriteNode> withBounds(
    const Stage& stage,
    const std::vector<std::string>& bounds) {
  const auto& write = rangeWrite(stage);
  const auto& spec = *std::dynamic_pointer_cast<
      const shuffle::RangePartitionFunctionSpec>(write.partitionFunctionSpec());
  return write.withPartitionFunction(spec.withBounds(bounds));
}

} // namespace ember::driver
