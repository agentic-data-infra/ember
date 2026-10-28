#include "ember/shuffle/RangePartitionFunction.h"

#include "velox/vector/DecodedVector.h"

namespace ember::shuffle {

namespace core = facebook::velox::core;
namespace velox = facebook::velox;

namespace {

// The key's first 8 bytes as a big-endian number.
uint64_t keyPrefix(velox::StringView key) {
  uint64_t prefix = 0;
  const auto size = std::min<size_t>(key.size(), sizeof(prefix));
  for (size_t i = 0; i < sizeof(prefix); ++i) {
    prefix <<= 8;
    if (i < size) {
      prefix |= static_cast<uint8_t>(key.data()[i]);
    }
  }
  return prefix;
}

} // namespace

RangePartitionFunction::RangePartitionFunction(
    int32_t numPartitions,
    velox::column_index_t keyChannel,
    bool ascending,
    bool nullsFirst)
    : numPartitions_{numPartitions},
      keyChannel_{keyChannel},
      ascending_{ascending},
      nullsFirst_{nullsFirst} {
  VELOX_CHECK_GT(numPartitions_, 0);
}

std::optional<uint32_t> RangePartitionFunction::partition(
    const velox::RowVector& input,
    std::vector<uint32_t>& partitions) {
  if (numPartitions_ == 1) {
    return 0;
  }
  const auto numRows = input.size();
  velox::DecodedVector key(*input.childAt(keyChannel_));
  partitions.resize(numRows);
  const uint32_t last = numPartitions_ - 1;
  for (velox::vector_size_t row = 0; row < numRows; ++row) {
    if (key.isNullAt(row)) {
      partitions[row] = nullsFirst_ ? 0 : last;
      continue;
    }
    // Slice of the 64-bit range: prefix * numPartitions / 2^64.
    const auto slice = static_cast<uint32_t>(
        (static_cast<unsigned __int128>(
             keyPrefix(key.valueAt<velox::StringView>(row))) *
         numPartitions_) >>
        64);
    partitions[row] = ascending_ ? slice : last - slice;
  }
  return std::nullopt;
}

std::unique_ptr<core::PartitionFunction> RangePartitionFunctionSpec::create(
    int numPartitions,
    bool /*localExchange*/) const {
  return std::make_unique<RangePartitionFunction>(
      numPartitions, keyChannel_, ascending_, nullsFirst_);
}

std::string RangePartitionFunctionSpec::toString() const {
  return fmt::format(
      "RANGE({} {} NULLS {})",
      keyChannel_,
      ascending_ ? "ASC" : "DESC",
      nullsFirst_ ? "FIRST" : "LAST");
}

folly::dynamic RangePartitionFunctionSpec::serialize() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["name"] = "RangePartitionFunctionSpec";
  obj["keyChannel"] = keyChannel_;
  obj["ascending"] = ascending_;
  obj["nullsFirst"] = nullsFirst_;
  return obj;
}

core::PartitionFunctionSpecPtr RangePartitionFunctionSpec::deserialize(
    const folly::dynamic& obj,
    void* /*context*/) {
  return std::make_shared<RangePartitionFunctionSpec>(
      obj["keyChannel"].asInt(),
      obj["ascending"].asBool(),
      obj["nullsFirst"].asBool());
}

} // namespace ember::shuffle
