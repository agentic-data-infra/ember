// The calling side of a connection: sends requests and completes a future per
// request when its response arrives. Any number of calls may be in flight; a
// reader thread matches responses to them by id.

#pragma once

#include <atomic>
#include <chrono>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

#include <folly/futures/Future.h>

#include "ember/rpc/Rpc.h"

namespace ember::rpc {

class RpcClient {
 public:
  /// Connects to 'address'. Throws RpcError if it cannot within 'timeout'.
  static std::shared_ptr<RpcClient> connect(
      const HostAndPort& address,
      std::chrono::milliseconds timeout = std::chrono::seconds(10));

  ~RpcClient();

  RpcClient(const RpcClient&) = delete;
  RpcClient& operator=(const RpcClient&) = delete;

  /// Sends a request. The future holds the response payload, or fails with
  /// RpcError if the server's handler failed or the connection broke.
  folly::SemiFuture<std::string> call(Method method, std::string payload);

  /// call() with an IOBuf payload, sent without copying.
  folly::SemiFuture<std::string> call(
      Method method,
      std::unique_ptr<folly::IOBuf> payload);

  const HostAndPort& address() const {
    return address_;
  }

  /// False once the connection broke or close() was called.
  bool isOpen() const {
    return !closed_;
  }

  /// Breaks the connection; pending calls fail.
  void close();

 private:
  RpcClient(HostAndPort address, int fd);

  void readLoop();
  void failPending(const std::string& reason);

  const HostAndPort address_;
  const int fd_;
  std::thread reader_;
  std::atomic<bool> closed_{false};
  std::mutex writeMutex_;
  std::mutex mutex_;
  uint64_t nextId_{1};
  std::map<uint64_t, folly::Promise<std::string>> pending_;
};

} // namespace ember::rpc
