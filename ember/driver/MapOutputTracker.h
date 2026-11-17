// Tracks where each map task's shuffle output lives. Reduce tasks get their
// inputs from it as block URLs.
//
// Outputs are lost with the executor that holds them (MapStatus::executorId);
// outputs in Kiln are not. Once an executor is marked lost, its outputs count
// as missing, and outputs reported from it later (by tasks that finished just
// before it died) are ignored. Executor ids are unique, so this holds even
// when a new executor serves at the lost one's location.

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
  /// executor is lost.
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

  /// Executor 'executorId' is gone: forget all its outputs, and ignore
  /// outputs reported from it later. Returns the number of outputs removed.
  int32_t markExecutorLost(const std::string& executorId);

  /// Block URLs of reduce partition 'reduceId' across all map outputs,
  /// skipping empty blocks.
  std::vector<std::string> blockUrls(int32_t shuffleId, int32_t reduceId) const;

  /// Block URLs of all partitions of map output 'mapId', skipping empty
  /// blocks.
  std::vector<std::string> mapBlockUrls(int32_t shuffleId, int32_t mapId) const;

  /// The map outputs of 'shuffleId' by map id, or std::nullopt if one is
  /// missing.
  std::optional<std::vector<shuffle::MapStatus>> mapStatuses(
      int32_t shuffleId) const;

  /// Total bytes per reduce partition.
  std::vector<int64_t> partitionSizes(int32_t shuffleId) const;

  void unregisterShuffle(int32_t shuffleId);

 private:
  mutable std::mutex mutex_;
  std::map<int32_t, std::vector<std::optional<shuffle::MapStatus>>> outputs_;
  int32_t removeOutputsAtLocked(
      std::vector<std::optional<shuffle::MapStatus>>& maps,
      const std::string& location);

  std::set<std::string> lostExecutors_;
};

} // namespace ember::driver
