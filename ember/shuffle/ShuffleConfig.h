// The session's shuffle settings. The driver reads these config keys, once,
// into a ShuffleTarget, which travels on each ShuffleWriteNode.

#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <string_view>

#include <folly/dynamic.h>

namespace ember::shuffle {

/// Bytes a map task buffers before spilling a run to disk.
inline constexpr std::string_view kSpillThreshold =
    "ember.shuffle.spill_threshold_bytes";

/// Whether a map task spills its buffered pages to run files on the
/// executor (true, the default) and writes its output at commit, or, with
/// the "kiln" store, appends them to its Kiln chunk as it runs (false), so
/// that a reduce partition's bytes are one range per append (see
/// MapStatus::segments).
inline constexpr std::string_view kShuffleSpill = "ember.shuffle.spill";

/// Where map outputs go: "file" (the default), the executor's shuffle files,
/// or "kiln", one Kiln chunk per map output (see KilnShuffle.h).
inline constexpr std::string_view kShuffleStore = "ember.shuffle.store";

/// The Kiln metadata server of the "kiln" store: Kiln's URL list,
/// "tcp://host:port,rdma://host:port", or just "host:port".
inline constexpr std::string_view kKilnMetadata = "ember.shuffle.kiln.metadata";

/// The Kiln tier the "kiln" store writes map outputs to: "dram" (the
/// default); "localfs", the storage servers' local disks; or "dram+localfs",
/// each map output into DRAM if it fits into Kiln's free DRAM, else local
/// disk, so that shuffles bigger than Kiln's DRAM neither fail nor evict.
inline constexpr std::string_view kKilnTier = "ember.shuffle.kiln.tier";

/// Where and how a map task writes its output.
struct ShuffleTarget {
  /// kShuffleStore.
  std::string store{"file"};
  /// kShuffleSpill.
  bool spill{true};
  /// kSpillThreshold.
  int64_t spillThreshold;
  /// kKilnMetadata, for the "kiln" store.
  std::string kilnMetadata;
  /// kKilnTier.
  std::string kilnTier{"dram"};
  /// The Kiln bucket of the shuffle. The driver creates one per shuffle and
  /// deletes it with the shuffle, which removes the shuffle's chunks from Kiln.
  uint64_t kilnBucket{0};

  ShuffleTarget();

  /// The target that session config 'config' asks for.
  static ShuffleTarget fromConfig(
      const std::map<std::string, std::string>& config);

  folly::dynamic serialize() const;
  static ShuffleTarget create(const folly::dynamic& obj);
};

} // namespace ember::shuffle
