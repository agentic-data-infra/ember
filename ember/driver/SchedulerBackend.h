// Where the driver's tasks run. The local backend runs them on executors in
// this process; a cluster backend sends them to worker processes. Either way
// tasks and results cross this interface as bytes.

#pragma once

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "ember/rpc/Messages.h"

namespace ember::driver {

class SchedulerBackend {
 public:
  using ResultCallback = std::function<void(std::string taskResult)>;

  /// Called with an executor's location (its shuffle outputs' location) when
  /// it joins or is lost, on any thread.
  struct ExecutorListener {
    std::function<void(const std::string& location)> onExecutorAdded;
    std::function<void(const std::string& location)> onExecutorLost;
  };

  virtual ~SchedulerBackend() = default;

  /// Tasks that run at once.
  virtual int32_t totalSlots() const = 0;

  /// Runs a serialized TaskDescription and calls 'onResult' with its
  /// serialized TaskResult, on any thread. Tasks beyond the free slots wait.
  /// A task with a 'node' runs only on an executor of that node.
  virtual void launchTask(
      std::string task,
      const std::string& node,
      ResultCallback onResult) = 0;

  /// The nodes (machines) executors run on, sorted.
  virtual std::vector<std::string> nodes() const = 0;

  /// The data files a scan of 'path' in file format 'schema' reads on 'node'.
  virtual std::vector<rpc::FileInfo> listFiles(
      const std::string& node,
      const std::string& schema,
      const std::string& path) = 0;

  /// Replaces the listener; an empty one removes it. Backends whose executors
  /// cannot be lost may ignore it.
  virtual void setExecutorListener(ExecutorListener /*listener*/) {}

  /// Deletes the files of 'shuffleId' on every executor.
  virtual void removeShuffle(int32_t shuffleId) = 0;

  /// Waits for running tasks and shuts the executors down.
  virtual void stop() = 0;
};

} // namespace ember::driver
