#include "ember/driver/TaskScheduler.h"

#include <condition_variable>
#include <mutex>

#include <glog/logging.h>

#include "velox/common/base/Exceptions.h"

namespace ember::driver {

TaskScheduler::TaskScheduler(
    std::shared_ptr<SchedulerBackend> backend,
    int32_t maxTaskAttempts)
    : backend_{std::move(backend)}, maxTaskAttempts_{maxTaskAttempts} {
  VELOX_CHECK_GT(maxTaskAttempts_, 0);
}

namespace {

struct TaskSet {
  std::vector<TaskDescription> tasks;
  TaskSetResult result;
  std::mutex mutex;
  std::condition_variable done;
  size_t pending{0};
  std::optional<std::string> failure;
  bool userError{false};
};

} // namespace

TaskSetResult TaskScheduler::runTaskSet(std::vector<TaskDescription> tasks) {
  auto set = std::make_shared<TaskSet>();
  set->tasks = std::move(tasks);
  set->result.results.resize(set->tasks.size());
  set->pending = set->tasks.size();

  std::function<void(size_t)> launch = [this, set, &launch](size_t index) {
    backend_->launchTask(
        set->tasks[index].serialize(),
        set->tasks[index].node,
        [this, set, index, &launch](std::string bytes) {
          auto result = TaskResult::deserialize(bytes);
          bool relaunch = false;
          {
            std::lock_guard<std::mutex> l(set->mutex);
            auto& task = set->tasks[index];
            if (result.state == TaskState::kFinished) {
              set->result.results[index] = std::move(result);
              --set->pending;
            } else if (result.fetchFailure.has_value()) {
              LOG(WARNING) << "Task " << result.taskId
                           << " lost its input: " << result.error;
              set->result.fetchFailures.push_back(*result.fetchFailure);
              --set->pending;
            } else if (
                // An error in the user's program fails the same way again.
                !result.userError && task.attempt + 1 < maxTaskAttempts_ &&
                !set->failure) {
              LOG(WARNING) << "Task " << result.taskId
                           << " failed, retrying: " << result.error;
              ++task.attempt;
              relaunch = true;
            } else {
              if (!set->failure) {
                set->failure = fmt::format(
                    "Task {} failed {} times. Last error: {}",
                    result.taskId,
                    task.attempt + 1,
                    result.error);
                set->userError = result.userError;
              }
              --set->pending;
            }
            if (set->pending == 0) {
              set->done.notify_all();
            }
          }
          if (relaunch) {
            launch(index);
          }
        });
  };

  for (size_t i = 0; i < set->tasks.size(); ++i) {
    launch(i);
  }
  std::unique_lock<std::mutex> l(set->mutex);
  set->done.wait(l, [&] { return set->pending == 0; });
  if (set->failure.has_value()) {
    if (set->userError) {
      VELOX_USER_FAIL(*set->failure);
    }
    VELOX_FAIL(*set->failure);
  }
  return std::move(set->result);
}

} // namespace ember::driver
