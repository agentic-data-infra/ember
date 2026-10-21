// Tracks where each map task's shuffle output lives. Reduce tasks get their
// inputs from it as block URLs.

#pragma once

#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "ember/shuffle/ShuffleInterface.h"

namespace ember::driver {

class MapOutputTracker {
 public:
  void registerShuffle(int32_t shuffleId, int32_t numMaps);

  void registerMapOutput(
      int32_t shuffleId,
      int32_t mapId,
      shuffle::MapStatus status);

  /// True once every map task of 'shuffleId' has committed.
  bool isComplete(int32_t shuffleId) const;

  /// Block URLs of reduce partition 'reduceId' across all map outputs,
  /// skipping empty blocks.
  std::vector<std::string> blockUrls(int32_t shuffleId, int32_t reduceId) const;

  /// Total bytes per reduce partition.
  std::vector<int64_t> partitionSizes(int32_t shuffleId) const;

  void unregisterShuffle(int32_t shuffleId);

 private:
  mutable std::mutex mutex_;
  std::map<int32_t, std::vector<std::optional<shuffle::MapStatus>>> outputs_;
};

} // namespace ember::driver
