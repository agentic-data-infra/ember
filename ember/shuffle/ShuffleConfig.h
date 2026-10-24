// Query config keys read by the shuffle operators.

#pragma once

#include <string_view>

namespace ember::shuffle {

/// Location of the executor running the task; ShuffleWrite writes to the
/// shuffle store registered under it.
inline constexpr std::string_view kExecutorLocation = "ember.executor_location";

/// Bytes a map task buffers before spilling a run to disk.
inline constexpr std::string_view kSpillThreshold =
    "ember.shuffle.spill_threshold_bytes";

/// Where map outputs go: "file" (the default), the executor's shuffle files,
/// or "kiln", one Kiln chunk per map output (see KilnShuffle.h).
inline constexpr std::string_view kShuffleStore = "ember.shuffle.store";

/// The Kiln metadata server ("host:port") of the "kiln" store.
inline constexpr std::string_view kKilnMetadata = "ember.shuffle.kiln.metadata";

} // namespace ember::shuffle
