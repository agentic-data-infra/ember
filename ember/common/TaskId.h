// Ember task ids: ember://<job>/<stage>.<stage attempt>/<partition>.<attempt>,
// unique per attempt. The Velox task of an Ember task carries it, so code
// that sees only the task id (e.g. a connector's data sink) can tell which
// partition it runs.

#pragma once

#include <cstdint>
#include <string>
#include <string_view>

namespace ember {

std::string formatTaskId(
    std::string_view jobId,
    int32_t stageId,
    int32_t stageAttempt,
    int32_t partition,
    int32_t attempt);

/// The partition of task 'taskId'. Throws if it is not an Ember task id.
int32_t taskPartition(std::string_view taskId);

} // namespace ember
