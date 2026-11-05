// RangePartitionFunction: split points, sort direction, nulls and serde.

#include <gtest/gtest.h>

#include "ember/shuffle/RangePartitionFunction.h"
#include "ember/shuffle/Shuffle.h"
#include "velox/vector/FlatVector.h"

namespace ember::shuffle {
namespace {

namespace velox = facebook::velox;

class RangePartitionFunctionTest : public testing::Test {
 protected:
  static void SetUpTestCase() {
    velox::memory::MemoryManager::testingSetInstance(
        velox::memory::MemoryManager::Options{});
    registerShuffle();
  }

  void SetUp() override {
    pool_ = velox::memory::memoryManager()->addLeafPool();
  }

  // One VARBINARY column 'key'; std::nullopt is a null.
  velox::RowVectorPtr keys(
      const std::vector<std::optional<std::string>>& values) {
    auto column = velox::BaseVector::create<velox::FlatVector<velox::StringView>>(
        velox::VARBINARY(), values.size(), pool_.get());
    for (size_t i = 0; i < values.size(); ++i) {
      if (values[i].has_value()) {
        column->set(i, velox::StringView(*values[i]));
      } else {
        column->setNull(i, true);
      }
    }
    return std::make_shared<velox::RowVector>(
        pool_.get(),
        velox::ROW({"key"}, {velox::VARBINARY()}),
        nullptr,
        values.size(),
        std::vector<velox::VectorPtr>{column});
  }

  static std::vector<uint32_t> partition(
      const RangePartitionFunctionSpec& spec,
      int numPartitions,
      const velox::RowVectorPtr& input) {
    std::vector<uint32_t> partitions;
    auto single =
        spec.create(numPartitions, false)->partition(*input, partitions);
    if (single.has_value()) {
      partitions.assign(input->size(), *single);
    }
    return partitions;
  }

  std::shared_ptr<velox::memory::MemoryPool> pool_;
};

TEST_F(RangePartitionFunctionTest, splitsAtBounds) {
  const std::vector<std::string> bounds{
      "b", std::string("c\0", 2), std::string("\xf0", 1)};
  auto input = keys(
      {std::string("a"),
       std::string("b"),
       std::string("bb"),
       std::string("c"),
       std::string("c\0", 2),
       std::string("d"),
       std::string("\xf0", 1),
       std::string("\xf1", 1),
       std::string(),
       std::nullopt});
  EXPECT_EQ(
      partition(RangePartitionFunctionSpec(0, true, true, bounds), 4, input),
      (std::vector<uint32_t>{0, 0, 1, 1, 1, 2, 2, 3, 0, 0}));
  EXPECT_EQ(
      partition(RangePartitionFunctionSpec(0, true, false, bounds), 4, input),
      (std::vector<uint32_t>{0, 0, 1, 1, 1, 2, 2, 3, 0, 3}));
  // Descending: the largest keys come first.
  EXPECT_EQ(
      partition(RangePartitionFunctionSpec(0, false, false, bounds), 4, input),
      (std::vector<uint32_t>{3, 3, 2, 2, 2, 1, 1, 0, 3, 3}));
  EXPECT_EQ(
      partition(RangePartitionFunctionSpec(0, true, true), 1, input),
      std::vector<uint32_t>(input->size(), 0));
}

TEST_F(RangePartitionFunctionTest, fewerBoundsThanPartitions) {
  auto input = keys({std::string("a"), std::string("z"), std::nullopt});
  const std::vector<std::string> bounds{"m"};
  EXPECT_EQ(
      partition(RangePartitionFunctionSpec(0, true, false, bounds), 4, input),
      (std::vector<uint32_t>{0, 1, 3}));
  EXPECT_EQ(
      partition(RangePartitionFunctionSpec(0, false, true, bounds), 4, input),
      (std::vector<uint32_t>{3, 2, 0}));
  EXPECT_EQ(
      partition(RangePartitionFunctionSpec(0, true, true), 4, input),
      (std::vector<uint32_t>{0, 0, 0}));
}

TEST_F(RangePartitionFunctionTest, serde) {
  RangePartitionFunctionSpec spec(2, false, true, {std::string("\0\xff", 2)});
  auto copy = velox::ISerializable::deserialize<
      facebook::velox::core::PartitionFunctionSpec>(spec.serialize(), nullptr);
  EXPECT_EQ(copy->toString(), "RANGE(2 DESC NULLS FIRST)");
  EXPECT_EQ(copy->serialize(), spec.serialize());
}

} // namespace
} // namespace ember::shuffle
