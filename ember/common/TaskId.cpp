#include "ember/TaskId.h"

#include <fmt/format.h>
#include <folly/Conv.h>

#include "velox/common/base/Exceptions.h"

namespace ember {

std::string formatTaskId(
    std::string_view jobId,
    int32_t stageId,
    int32_t stageAttempt,
    int32_t partition,
    int32_t attempt) {
  return fmt::format(
      "ember://{}/{}.{}/{}.{}",
      jobId,
      stageId,
      stageAttempt,
      partition,
      attempt);
}

int32_t taskPartition(std::string_view taskId) {
  // The last path element is <partition>.<attempt>.
  const auto slash = taskId.rfind('/');
  const auto dot = taskId.rfind('.');
  VELOX_CHECK(
      taskId.starts_with("ember://") && slash != std::string_view::npos &&
          dot != std::string_view::npos && dot > slash,
      "Not an Ember task id: {}",
      taskId);
  return folly::to<int32_t>(taskId.substr(slash + 1, dot - slash - 1));
}

} // namespace ember
