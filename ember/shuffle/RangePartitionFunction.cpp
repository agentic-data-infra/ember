#include "ember/shuffle/RangePartitionFunction.h"

#include <folly/String.h>

#include <algorithm>
#include <string_view>

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
    bool nullsFirst,
    std::vector<std::string> bounds)
    : numPartitions_{numPartitions},
      keyChannel_{keyChannel},
      ascending_{ascending},
      nullsFirst_{nullsFirst},
      bounds_{std::move(bounds)} {
  VELOX_CHECK_GT(numPartitions_, 0);
  VELOX_CHECK_LT(bounds_.size(), numPartitions_);
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
    const auto value = key.valueAt<velox::StringView>(row);
    uint32_t range;
    if (bounds_.empty()) {
      // Slice of the 64-bit range: prefix * numPartitions / 2^64.
      range = static_cast<uint32_t>(
          (static_cast<unsigned __int128>(keyPrefix(value)) * numPartitions_) >>
          64);
    } else {
      range = std::lower_bound(
                  bounds_.begin(), bounds_.end(), std::string_view(value)) -
          bounds_.begin();
    }
    partitions[row] = ascending_ ? range : last - range;
  }
  return std::nullopt;
}

std::unique_ptr<core::PartitionFunction> RangePartitionFunctionSpec::create(
    int numPartitions,
    bool /*localExchange*/) const {
  return std::make_unique<RangePartitionFunction>(
      numPartitions, keyChannel_, ascending_, nullsFirst_, bounds_);
}

std::shared_ptr<const RangePartitionFunctionSpec>
RangePartitionFunctionSpec::withBounds(std::vector<std::string> bounds) const {
  return std::make_shared<RangePartitionFunctionSpec>(
      keyChannel_, ascending_, nullsFirst_, std::move(bounds));
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
  // Keys are binary: JSON carries them in hex.
  folly::dynamic bounds = folly::dynamic::array;
  for (const auto& bound : bounds_) {
    bounds.push_back(folly::hexlify(bound));
  }
  obj["bounds"] = std::move(bounds);
  return obj;
}

core::PartitionFunctionSpecPtr RangePartitionFunctionSpec::deserialize(
    const folly::dynamic& obj,
    void* /*context*/) {
  std::vector<std::string> bounds;
  for (const auto& bound : obj["bounds"]) {
    bounds.emplace_back();
    VELOX_CHECK(folly::unhexlify(bound.asString(), bounds.back()));
  }
  return std::make_shared<RangePartitionFunctionSpec>(
      obj["keyChannel"].asInt(),
      obj["ascending"].asBool(),
      obj["nullsFirst"].asBool(),
      std::move(bounds));
}

} // namespace ember::shuffle
