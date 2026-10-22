// What the driver and workers implement to serve RPC (RpcServer). Methods are
// called on the server's threads; one that blocks or runs long (a task, a
// disk read) should move its work to an executor and return the future. A
// method that throws, or whose future fails, fails the call with the
// exception's message.

#pragma once

#include <memory>
#include <string>

#include <folly/futures/Future.h>
#include <folly/io/IOBuf.h>

#include "ember/rpc/Messages.h"

namespace ember::rpc {

/// Served by the driver, at its master URL ember://host:port.
class DriverHandler {
 public:
  virtual ~DriverHandler() = default;

  virtual RegisterWorkerResponse registerWorker(
      const RegisterWorkerRequest& request) = 0;

  /// Returns false if the driver no longer counts 'workerId' as registered,
  /// e.g. after it missed heartbeats. The worker then registers again.
  virtual bool heartbeat(const std::string& workerId) = 0;
};

/// Served by every worker: tasks from the driver it registered with, and its
/// shuffle blocks to other workers.
class WorkerHandler {
 public:
  virtual ~WorkerHandler() = default;

  /// Runs a TaskDescription. The result is its TaskResult.
  virtual folly::SemiFuture<std::string> launchTask(std::string task) = 0;

  /// Deletes a shuffle's files.
  virtual void removeShuffle(int32_t shuffleId) = 0;

  /// The application is over.
  virtual void shutdown() = 0;

  /// The bytes of one shuffle block this worker's executor wrote.
  virtual folly::SemiFuture<std::unique_ptr<folly::IOBuf>> fetchBlock(
      const BlockId& block) = 0;
};

} // namespace ember::rpc
