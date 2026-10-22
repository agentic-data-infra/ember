// Runs tasks on worker processes, which may be on other hosts.
//
// The driver serves RPC on 'port' (master URL ember://host:port). Workers,
// started separately, register there with their address and task slots; the
// backend connects back to each to launch tasks and sends tasks to workers
// with free slots. A task's result is the reply to its launchTask call.
//
// A worker that misses heartbeats for 'heartbeatTimeout', or whose connection
// breaks, is dropped: its running tasks fail and the TaskScheduler retries
// them elsewhere, and the executor listener hears that its location is lost,
// so the DagScheduler recomputes the shuffle outputs it held.

#pragma once

#include <chrono>
#include <condition_variable>
#include <deque>
#include <map>
#include <mutex>
#include <thread>

#include "ember/driver/SchedulerBackend.h"
#include "ember/rpc/Clients.h"
#include "ember/rpc/RpcServer.h"

namespace folly {
class CPUThreadPoolExecutor;
}

namespace ember::driver {

struct ClusterBackendOptions {
  std::string appName{"ember"};
  /// Interface to serve on; empty for all.
  std::string bindHost;
  /// 0 picks a free port.
  uint16_t port{0};
  std::chrono::milliseconds heartbeatInterval{std::chrono::seconds(2)};
  std::chrono::milliseconds heartbeatTimeout{std::chrono::seconds(30)};
  /// Tasks fail if no worker is registered for this long.
  std::chrono::milliseconds noWorkersTimeout{std::chrono::seconds(60)};
};

class ClusterBackend : public SchedulerBackend {
 public:
  /// Starts serving. Throws if the port is taken.
  explicit ClusterBackend(ClusterBackendOptions options);
  ~ClusterBackend() override;

  /// The port workers register at.
  uint16_t port() const {
    return server_.port();
  }

  /// Waits until 'count' workers are registered. Throws on timeout.
  void waitForWorkers(int32_t count, std::chrono::milliseconds timeout);

  /// Addresses of the registered workers.
  std::vector<std::string> workers() const;

  int32_t totalSlots() const override;

  void launchTask(std::string task, ResultCallback onResult) override;

  void setExecutorListener(ExecutorListener listener) override;

  void removeShuffle(int32_t shuffleId) override;

  /// Tells the workers the application is over and stops serving.
  void stop() override;

 private:
  // Serves the workers' calls.
  class Handler;

  struct WorkerInfo {
    std::string id;
    std::string address;
    int32_t slots{0};
    int32_t running{0};
    std::shared_ptr<rpc::WorkerClient> client;
    std::chrono::steady_clock::time_point lastHeartbeat;
  };

  struct PendingTask {
    std::string task;
    ResultCallback onResult;
  };

  rpc::RegisterWorkerResponse registerWorker(
      const rpc::RegisterWorkerRequest& request);
  bool heartbeat(const std::string& workerId);

  // Sends pending tasks to free slots.
  void dispatch();
  void onTaskDone(
      const std::shared_ptr<WorkerInfo>& worker,
      PendingTask task,
      folly::Try<std::string> result);
  void removeWorkerLocked(const std::shared_ptr<WorkerInfo>& worker,
                          const std::string& reason);
  void monitor();

  static void fail(PendingTask task, const std::string& error);

  const ClusterBackendOptions options_;
  rpc::RpcServer server_;
  std::unique_ptr<folly::CPUThreadPoolExecutor> callbacks_;
  std::thread monitor_;

  mutable std::mutex mutex_;
  std::condition_variable changed_;
  bool stopped_{false};
  std::map<std::string, std::shared_ptr<WorkerInfo>> workers_;
  std::deque<PendingTask> pending_;
  std::chrono::steady_clock::time_point noWorkersSince_;
  ExecutorListener listener_;
};

} // namespace ember::driver
