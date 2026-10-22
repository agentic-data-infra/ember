// Tracks where each map task's shuffle output lives. Reduce tasks get their
// inputs from it as block URLs.
//
// Outputs are lost with the executor that holds them. Once a location is
// marked lost, its outputs count as missing, and outputs reported from it
// later (by tasks that finished just before it died) are ignored, until an
// executor registers at that location again.

#pragma once

#include <map>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <vector>

#include "ember/shuffle/ShuffleInterface.h"

namespace ember::driver {

class MapOutputTracker {
 public:
  void registerShuffle(int32_t shuffleId, int32_t numMaps);

  /// Records a map task's output. Returns false, recording nothing, if its
  /// location is lost.
  bool registerMapOutput(
      int32_t shuffleId,
      int32_t mapId,
      shuffle::MapStatus status);

  /// Map tasks of 'shuffleId', or std::nullopt if it is not registered.
  std::optional<int32_t> numMaps(int32_t shuffleId) const;

  /// True once every map task of 'shuffleId' has an output.
  bool isComplete(int32_t shuffleId) const;

  /// Map ids of 'shuffleId' without an output, in order.
  std::vector<int32_t> missingMaps(int32_t shuffleId) const;

  /// Forgets the outputs of 'shuffleId' at 'location', e.g. after a reduce
  /// task could not fetch from it. Returns the number of outputs removed.
  int32_t removeOutputsAt(int32_t shuffleId, const std::string& location);

  /// The executor at 'location' is gone: forget all its outputs, and ignore
  /// outputs reported from it until markLocationAvailable(). Returns the
  /// number of outputs removed.
  int32_t markLocationLost(const std::string& location);

  /// An executor serves at 'location' again.
  void markLocationAvailable(const std::string& location);

  /// Block URLs of reduce partition 'reduceId' across all map outputs,
  /// skipping empty blocks.
  std::vector<std::string> blockUrls(int32_t shuffleId, int32_t reduceId) const;

  /// Block URLs of all partitions of map output 'mapId', skipping empty
  /// blocks.
  std::vector<std::string> mapBlockUrls(int32_t shuffleId, int32_t mapId) const;

  /// Total bytes per reduce partition.
  std::vector<int64_t> partitionSizes(int32_t shuffleId) const;

  void unregisterShuffle(int32_t shuffleId);

 private:
  mutable std::mutex mutex_;
  std::map<int32_t, std::vector<std::optional<shuffle::MapStatus>>> outputs_;
  int32_t removeOutputsAtLocked(
      std::vector<std::optional<shuffle::MapStatus>>& maps,
      const std::string& location);

  std::set<std::string> lostLocations_;
};

} // namespace ember::driver
