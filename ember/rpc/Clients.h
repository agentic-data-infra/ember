// The calling side of Ember's RPC: a client per service, usable from any
// thread.
//
// Calls are coroutines: a call is sent when its task is awaited, and the
// client must outlive it. Any number of calls may be in flight on a client
// and complete in any order. Calls fail with RpcError: when the server's
// handler threw, when the connection broke (which fails every pending
// call), or after close(). There is no default call timeout, since a task
// runs as long as it takes; callers bound their waits.

#pragma once

#include <chrono>
#include <memory>
#include <string>

#include <folly/coro/Task.h>
#include <folly/io/IOBuf.h>

#include "ember/rpc/Messages.h"
#include "ember/rpc/Rpc.h"

namespace ember::rpc {

namespace detail {
class Connection;
}

/// A connection to a server; the clients below add its service's calls.
class Client {
 public:
  virtual ~Client();

  Client(const Client&) = delete;
  Client& operator=(const Client&) = delete;

  const HostAndPort& address() const;

  /// False once the connection broke or close() was called.
  bool isOpen() const;

  /// Breaks the connection; pending calls fail.
  void close();

 protected:
  /// Connects to 'address'. Throws RpcError if it cannot within 'timeout'.
  Client(const HostAndPort& address, std::chrono::milliseconds timeout);

  const std::unique_ptr<detail::Connection> connection_;
};

constexpr std::chrono::milliseconds kDefaultConnectTimeout{10'000};

/// Calls a driver (DriverHandler).
class DriverClient : public Client {
 public:
  static std::shared_ptr<DriverClient> connect(
      const HostAndPort& address,
      std::chrono::milliseconds timeout = kDefaultConnectTimeout);

  ~DriverClient() override;

  folly::coro::Task<RegisterWorkerResponse> co_registerWorker(
      RegisterWorkerRequest request);

  /// Whether the driver still counts 'workerId' as registered.
  folly::coro::Task<bool> co_heartbeat(std::string workerId);

 private:
  struct Stub;

  DriverClient(const HostAndPort& address, std::chrono::milliseconds timeout);

  std::unique_ptr<Stub> stub_;
};

/// Calls a worker on behalf of its driver (WorkerHandler).
class WorkerClient : public Client {
 public:
  static std::shared_ptr<WorkerClient> connect(
      const HostAndPort& address,
      std::chrono::milliseconds timeout = kDefaultConnectTimeout);

  ~WorkerClient() override;

  /// Runs a TaskDescription; the result is its TaskResult.
  folly::coro::Task<std::string> co_launchTask(std::string task);

  folly::coro::Task<void> co_removeShuffle(int32_t shuffleId);

  folly::coro::Task<void> co_shutdown();

 private:
  struct Stub;

  WorkerClient(const HostAndPort& address, std::chrono::milliseconds timeout);

  std::unique_ptr<Stub> stub_;
};

/// Fetches shuffle blocks from a worker (WorkerHandler::fetchBlock).
class ShuffleBlockClient : public Client {
 public:
  static std::shared_ptr<ShuffleBlockClient> connect(
      const HostAndPort& address,
      std::chrono::milliseconds timeout = kDefaultConnectTimeout);

  ~ShuffleBlockClient() override;

  folly::coro::Task<std::unique_ptr<folly::IOBuf>> co_fetchBlock(
      BlockId block);

 private:
  struct Stub;

  ShuffleBlockClient(
      const HostAndPort& address,
      std::chrono::milliseconds timeout);

  std::unique_ptr<Stub> stub_;
};

} // namespace ember::rpc
