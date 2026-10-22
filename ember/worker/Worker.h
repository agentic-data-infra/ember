// Worker: the process side of an executor. It serves tasks and shuffle blocks
// on its own port and offers its task slots to a driver.
//
// A worker starts its RPC server, then registers with the driver at
// 'driver', retrying until the driver is up, and sends heartbeats. For each
// driver it registers with it creates a fresh Executor, with its own shuffle
// directory under 'localDirectory', and removes it when that driver is done:
// the driver sent kShutdown, stopped answering heartbeats, or forgot the
// worker. Then the worker exits, or with 'persistent' waits for the next
// driver at the same address.
//
// Its address ("host:port") is also its executor's location, so shuffle block
// URLs name the worker that holds the block. Nothing here assumes the driver
// or other workers share the host.

#pragma once

#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>

#include "ember/rpc/RpcClient.h"
#include "ember/rpc/RpcServer.h"

namespace folly {
class CPUThreadPoolExecutor;
}

namespace ember {

class Executor;
class NetworkBlockTransport;

struct WorkerOptions {
  /// The driver's RPC address.
  rpc::HostAndPort driver;
  /// Interface to serve on; empty for all.
  std::string bindHost;
  /// Host name others reach this worker by. Defaults to the host name.
  std::string advertisedHost;
  /// 0 picks a free port.
  uint16_t port{0};
  /// Tasks run at once.
  int32_t slots{1};
  /// Threads running Velox drivers; 0 for max(4, 2 * slots).
  int32_t numThreads{0};
  /// Parent of the shuffle directories.
  std::string localDirectory;
  /// Keep serving after a driver is done, waiting for the next one.
  bool persistent{false};
  /// How long to retry reaching the driver; zero retries forever.
  std::chrono::milliseconds connectTimeout{std::chrono::seconds(60)};
  /// Fetch blocks of this process's own executor over TCP too.
  bool fetchLocalBlocksOverNetwork{false};
  /// A shuffle block fetch fails after this long; the driver then recomputes
  /// the block.
  std::chrono::milliseconds fetchTimeout{std::chrono::seconds(120)};
};

class Worker {
 public:
  explicit Worker(WorkerOptions options);
  ~Worker();

  /// Starts serving. Returns the worker's address.
  std::string start();

  /// Registers with the driver and serves it until it is done, then, if
  /// persistent, with the next one. Returns once the worker should exit.
  void run();

  /// Makes run() return. Thread-safe.
  void stop();

  const std::string& address() const {
    return address_;
  }

 private:
  // Serves one driver from registration until it is done. Returns false if
  // the driver could not be reached.
  bool serveDriver(std::chrono::milliseconds connectTimeout);
  void startExecutor();
  void stopExecutor();

  folly::SemiFuture<std::unique_ptr<folly::IOBuf>> launchTask(
      std::string task);

  const WorkerOptions options_;
  std::string address_;
  int32_t numThreads_;
  rpc::RpcServer server_;
  std::shared_ptr<NetworkBlockTransport> transport_;
  // Reads blocks for other workers.
  std::unique_ptr<folly::CPUThreadPoolExecutor> blockReaders_;

  std::mutex mutex_;
  std::condition_variable wakeup_;
  bool stopping_{false};
  bool driverDone_{false};
  int32_t registrations_{0};
  // The current driver's executor and task slots.
  std::shared_ptr<Executor> executor_;
  std::shared_ptr<folly::CPUThreadPoolExecutor> slots_;
};

} // namespace ember
