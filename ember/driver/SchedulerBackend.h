// Where the driver's tasks run. The local backend runs them on executors in
// this process; a cluster backend sends them to worker processes. Either way
// tasks and results cross this interface as bytes.

#pragma once

#include <functional>
#include <memory>
#include <string>

namespace ember::driver {

class SchedulerBackend {
 public:
  using ResultCallback = std::function<void(std::string taskResult)>;

  virtual ~SchedulerBackend() = default;

  /// Tasks that run at once.
  virtual int32_t totalSlots() const = 0;

  /// Runs a serialized TaskDescription and calls 'onResult' with its
  /// serialized TaskResult, on any thread. Tasks beyond the free slots wait.
  virtual void launchTask(std::string task, ResultCallback onResult) = 0;

  /// Deletes the files of 'shuffleId' on every executor.
  virtual void removeShuffle(int32_t shuffleId) = 0;

  /// Waits for running tasks and shuts the executors down.
  virtual void stop() = 0;
};

} // namespace ember::driver
