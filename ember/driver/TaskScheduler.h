// Runs the tasks of one stage on a backend, as a task set: launches every
// task, collects results, and relaunches a failed task as a new attempt until
// it has failed 'maxTaskAttempts' times. Errors in the user's program are not
// retried.

#pragma once

#include <memory>
#include <vector>

#include "ember/driver/SchedulerBackend.h"
#include "ember/worker/TaskDescription.h"

namespace ember::driver {

class TaskScheduler {
 public:
  TaskScheduler(std::shared_ptr<SchedulerBackend> backend, int32_t maxTaskAttempts);

  /// Runs 'tasks' (one per partition, in partition order) and returns their
  /// results in the same order. Throws once a task fails 'maxTaskAttempts'
  /// times, after the tasks still running have finished.
  std::vector<TaskResult> runTaskSet(std::vector<TaskDescription> tasks);

  SchedulerBackend& backend() {
    return *backend_;
  }

 private:
  const std::shared_ptr<SchedulerBackend> backend_;
  const int32_t maxTaskAttempts_;
};

} // namespace ember::driver
