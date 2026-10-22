// Runs the tasks of one stage on a backend, as a task set: launches every
// task, collects results, and relaunches a failed task as a new attempt until
// it has failed 'maxTaskAttempts' times. Errors in the user's program are not
// retried. Neither are fetch failures: a task that cannot read a shuffle block
// would fail again until the block's map output is recomputed, which is the
// DagScheduler's job, so the task set reports them instead.

#pragma once

#include <memory>
#include <optional>
#include <vector>

#include "ember/driver/SchedulerBackend.h"
#include "ember/worker/TaskDescription.h"

namespace ember::driver {

struct TaskSetResult {
  /// Per task, in partition order; empty for a task that hit a fetch failure.
  std::vector<std::optional<TaskResult>> results;
  /// The lost blocks, one per failed task.
  std::vector<shuffle::FetchFailure> fetchFailures;
};

class TaskScheduler {
 public:
  TaskScheduler(std::shared_ptr<SchedulerBackend> backend, int32_t maxTaskAttempts);

  /// Runs 'tasks' and returns their results in the same order, with the
  /// fetch failures of those that could not read their input. Throws once a
  /// task fails 'maxTaskAttempts' times, after the tasks still running have
  /// finished.
  TaskSetResult runTaskSet(std::vector<TaskDescription> tasks);

  SchedulerBackend& backend() {
    return *backend_;
  }

 private:
  const std::shared_ptr<SchedulerBackend> backend_;
  const int32_t maxTaskAttempts_;
};

} // namespace ember::driver
