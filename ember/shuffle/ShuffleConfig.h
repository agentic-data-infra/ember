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

} // namespace ember::shuffle
