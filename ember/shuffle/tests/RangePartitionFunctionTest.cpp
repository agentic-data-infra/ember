// RangePartitionFunction: fixed key-space slices, sort direction, nulls and
// serde.

#include <gtest/gtest.h>

#include <random>

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

TEST_F(RangePartitionFunctionTest, slicesTheKeySpace) {
  auto input = keys(
      {std::string(10, '\0'),
       std::string("\x3f\xff", 2),
       std::string("\x40", 1),
       std::string("\x80\x00\x00\x00\x00\x00\x00\x00\x01", 9),
       std::string("\xbf", 1),
       std::string(10, '\xff'),
       std::string(),
       std::nullopt});
  EXPECT_EQ(
      partition(RangePartitionFunctionSpec(0, true, true), 4, input),
      (std::vector<uint32_t>{0, 0, 1, 2, 2, 3, 0, 0}));
  EXPECT_EQ(
      partition(RangePartitionFunctionSpec(0, true, false), 4, input),
      (std::vector<uint32_t>{0, 0, 1, 2, 2, 3, 0, 3}));
  // Descending: the largest keys come first.
  EXPECT_EQ(
      partition(RangePartitionFunctionSpec(0, false, false), 4, input),
      (std::vector<uint32_t>{3, 3, 2, 1, 1, 0, 3, 3}));
  EXPECT_EQ(
      partition(RangePartitionFunctionSpec(0, true, true), 1, input),
      std::vector<uint32_t>(input->size(), 0));
}

TEST_F(RangePartitionFunctionTest, keepsSortOrderAndBalancesRandomKeys) {
  std::mt19937 rng(7);
  std::vector<std::optional<std::string>> values;
  for (int i = 0; i < 10'000; ++i) {
    std::string key(10, '\0');
    for (auto& c : key) {
      c = static_cast<char>(rng());
    }
    values.push_back(key);
  }
  auto input = keys(values);
  constexpr int kPartitions = 8;
  auto partitions =
      partition(RangePartitionFunctionSpec(0, true, true), kPartitions, input);

  std::vector<int32_t> counts(kPartitions);
  for (auto p : partitions) {
    ++counts[p];
  }
  for (auto count : counts) {
    EXPECT_NEAR(count, 10'000 / kPartitions, 200);
  }
  // A smaller key never lands in a later partition.
  for (size_t i = 0; i < values.size(); ++i) {
    for (size_t j = i + 1; j < std::min(values.size(), i + 50); ++j) {
      if (*values[i] < *values[j]) {
        EXPECT_LE(partitions[i], partitions[j]);
      } else {
        EXPECT_GE(partitions[i], partitions[j]);
      }
    }
  }
}

TEST_F(RangePartitionFunctionTest, serde) {
  RangePartitionFunctionSpec spec(2, false, true);
  auto copy = velox::ISerializable::deserialize<
      facebook::velox::core::PartitionFunctionSpec>(spec.serialize(), nullptr);
  EXPECT_EQ(copy->toString(), "RANGE(2 DESC NULLS FIRST)");
  EXPECT_EQ(copy->serialize(), spec.serialize());
}

} // namespace
} // namespace ember::shuffle
