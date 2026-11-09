#include "ember/shuffle/ShuffleConfig.h"

#include <folly/Conv.h>

#include "velox/common/base/Exceptions.h"

namespace ember::shuffle {

ShuffleTarget ShuffleTarget::fromConfig(
    const std::map<std::string, std::string>& config) {
  ShuffleTarget target;
  auto value = [&](std::string_view key) -> const std::string* {
    auto it = config.find(std::string(key));
    return it == config.end() ? nullptr : &it->second;
  };
  if (const auto* store = value(kShuffleStore)) {
    target.store = *store;
  }
  if (const auto* spill = value(kShuffleSpill)) {
    target.spill = folly::to<bool>(*spill);
  }
  if (const auto* threshold = value(kSpillThreshold)) {
    target.spillThreshold = folly::to<int64_t>(*threshold);
  }
  if (const auto* tier = value(kKilnTier)) {
    target.kilnTier = *tier;
  }
  if (target.store == "kiln") {
    const auto* metadata = value(kKilnMetadata);
    VELOX_USER_CHECK_NOT_NULL(
        metadata, "{}=kiln needs config {}", kShuffleStore, kKilnMetadata);
    target.kilnMetadata = *metadata;
  } else {
    VELOX_USER_CHECK_EQ(target.store, "file", "Unknown {}", kShuffleStore);
    VELOX_USER_CHECK(
        target.spill, "{}=false needs {}=kiln", kShuffleSpill, kShuffleStore);
  }
  return target;
}

folly::dynamic ShuffleTarget::serialize() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["store"] = store;
  obj["spill"] = spill;
  if (spillThreshold.has_value()) {
    obj["spillThreshold"] = *spillThreshold;
  }
  obj["kilnMetadata"] = kilnMetadata;
  obj["kilnTier"] = kilnTier;
  obj["kilnBucket"] = static_cast<int64_t>(kilnBucket);
  return obj;
}

ShuffleTarget ShuffleTarget::create(const folly::dynamic& obj) {
  ShuffleTarget target;
  target.store = obj["store"].asString();
  target.spill = obj["spill"].asBool();
  if (const auto* threshold = obj.get_ptr("spillThreshold")) {
    target.spillThreshold = threshold->asInt();
  }
  target.kilnMetadata = obj["kilnMetadata"].asString();
  target.kilnTier = obj["kilnTier"].asString();
  target.kilnBucket = static_cast<uint64_t>(obj["kilnBucket"].asInt());
  return target;
}

} // namespace ember::shuffle
