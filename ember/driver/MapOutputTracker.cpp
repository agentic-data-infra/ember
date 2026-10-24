#include "ember/driver/MapOutputTracker.h"

#include <numeric>

#include "velox/common/base/Exceptions.h"

namespace ember::driver {

void MapOutputTracker::registerShuffle(int32_t shuffleId, int32_t numMaps) {
  std::lock_guard<std::mutex> l(mutex_);
  outputs_[shuffleId].assign(numMaps, std::nullopt);
}

bool MapOutputTracker::registerMapOutput(
    int32_t shuffleId,
    int32_t mapId,
    shuffle::MapStatus status) {
  std::lock_guard<std::mutex> l(mutex_);
  auto& maps = outputs_.at(shuffleId);
  VELOX_CHECK_LT(mapId, maps.size());
  if (lostLocations_.contains(status.location)) {
    return false;
  }
  maps[mapId] = std::move(status);
  return true;
}

std::vector<int32_t> MapOutputTracker::missingMaps(int32_t shuffleId) const {
  std::lock_guard<std::mutex> l(mutex_);
  const auto& maps = outputs_.at(shuffleId);
  std::vector<int32_t> missing;
  for (int32_t mapId = 0; mapId < maps.size(); ++mapId) {
    if (!maps[mapId].has_value()) {
      missing.push_back(mapId);
    }
  }
  return missing;
}

int32_t MapOutputTracker::removeOutputsAt(
    int32_t shuffleId,
    const std::string& location) {
  std::lock_guard<std::mutex> l(mutex_);
  auto it = outputs_.find(shuffleId);
  return it == outputs_.end() ? 0 : removeOutputsAtLocked(it->second, location);
}

int32_t MapOutputTracker::markLocationLost(const std::string& location) {
  std::lock_guard<std::mutex> l(mutex_);
  lostLocations_.insert(location);
  int32_t removed = 0;
  for (auto& [shuffleId, maps] : outputs_) {
    removed += removeOutputsAtLocked(maps, location);
  }
  return removed;
}

int32_t MapOutputTracker::removeOutputsAtLocked(
    std::vector<std::optional<shuffle::MapStatus>>& maps,
    const std::string& location) {
  int32_t removed = 0;
  for (auto& status : maps) {
    if (status.has_value() && status->location == location) {
      status.reset();
      ++removed;
    }
  }
  return removed;
}

void MapOutputTracker::markLocationAvailable(const std::string& location) {
  std::lock_guard<std::mutex> l(mutex_);
  lostLocations_.erase(location);
}

std::optional<int32_t> MapOutputTracker::numMaps(int32_t shuffleId) const {
  std::lock_guard<std::mutex> l(mutex_);
  auto it = outputs_.find(shuffleId);
  if (it == outputs_.end()) {
    return std::nullopt;
  }
  return static_cast<int32_t>(it->second.size());
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

std::vector<std::string> MapOutputTracker::mapBlockUrls(
    int32_t shuffleId,
    int32_t mapId) const {
  std::lock_guard<std::mutex> l(mutex_);
  const auto& map = outputs_.at(shuffleId).at(mapId);
  VELOX_CHECK(
      map.has_value(),
      "Map output {} of shuffle {} is missing",
      mapId,
      shuffleId);
  std::vector<std::string> urls;
  int64_t offset = 0;
  for (int32_t p = 0; p < map->partitionLengths.size(); ++p) {
    const auto length = map->partitionLengths[p];
    if (length > 0) {
      urls.push_back(shuffle::blockUrl(
          map->location, {shuffleId, mapId, p}, offset, length));
    }
    offset += length;
  }
  return urls;
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
      const auto offset = std::accumulate(
          lengths.begin(), lengths.begin() + reduceId, int64_t{0});
      urls.push_back(shuffle::blockUrl(
          maps[mapId]->location,
          {shuffleId, mapId, reduceId},
          offset,
          lengths[reduceId]));
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
