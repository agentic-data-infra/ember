#include "ember/driver/MapOutputTracker.h"

#include "velox/common/base/Exceptions.h"

namespace ember::driver {

void MapOutputTracker::registerShuffle(int32_t shuffleId, int32_t numMaps) {
  std::lock_guard<std::mutex> l(mutex_);
  outputs_[shuffleId].assign(numMaps, std::nullopt);
}

void MapOutputTracker::registerMapOutput(
    int32_t shuffleId,
    int32_t mapId,
    shuffle::MapStatus status) {
  std::lock_guard<std::mutex> l(mutex_);
  auto& maps = outputs_.at(shuffleId);
  VELOX_CHECK_LT(mapId, maps.size());
  maps[mapId] = std::move(status);
}

bool MapOutputTracker::isComplete(int32_t shuffleId) const {
  std::lock_guard<std::mutex> l(mutex_);
  for (const auto& status : outputs_.at(shuffleId)) {
    if (!status.has_value()) {
      return false;
    }
  }
  return true;
}

std::vector<std::string> MapOutputTracker::blockUrls(
    int32_t shuffleId,
    int32_t reduceId) const {
  std::lock_guard<std::mutex> l(mutex_);
  const auto& maps = outputs_.at(shuffleId);
  std::vector<std::string> urls;
  for (int32_t mapId = 0; mapId < maps.size(); ++mapId) {
    VELOX_CHECK(
        maps[mapId].has_value(),
        "Map output {} of shuffle {} is missing",
        mapId,
        shuffleId);
    const auto& lengths = maps[mapId]->partitionLengths;
    VELOX_CHECK_LT(reduceId, lengths.size());
    if (lengths[reduceId] > 0) {
      urls.push_back(shuffle::blockUrl(
          maps[mapId]->location, {shuffleId, mapId, reduceId}));
    }
  }
  return urls;
}

std::vector<int64_t> MapOutputTracker::partitionSizes(int32_t shuffleId) const {
  std::lock_guard<std::mutex> l(mutex_);
  std::vector<int64_t> sizes;
  for (const auto& status : outputs_.at(shuffleId)) {
    if (!status.has_value()) {
      continue;
    }
    sizes.resize(
        std::max(sizes.size(), status->partitionLengths.size()), 0);
    for (size_t i = 0; i < status->partitionLengths.size(); ++i) {
      sizes[i] += status->partitionLengths[i];
    }
  }
  return sizes;
}

void MapOutputTracker::unregisterShuffle(int32_t shuffleId) {
  std::lock_guard<std::mutex> l(mutex_);
  outputs_.erase(shuffleId);
}

} // namespace ember::driver
