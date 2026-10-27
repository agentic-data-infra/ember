// The serving side of Ember's RPC: a driver's or a worker's handler
// (Handlers.h) on a port.

#pragma once

#include <memory>
#include <string>

#include "ember/rpc/Handlers.h"
#include "ember/rpc/Rpc.h"

namespace ember::rpc {

class RpcServer {
 public:
  RpcServer();
  ~RpcServer();

  RpcServer(const RpcServer&) = delete;
  RpcServer& operator=(const RpcServer&) = delete;

  /// Starts serving 'handler' on 'host' (empty for all interfaces) and
  /// 'port'; port 0 picks a free one. Throws RpcError if it cannot listen.
  void start(
      std::shared_ptr<DriverHandler> handler,
      const std::string& host,
      uint16_t port);

  /// start() for a worker.
  void start(
      std::shared_ptr<WorkerHandler> handler,
      const std::string& host,
      uint16_t port);

  /// The port served, once started.
  uint16_t port() const {
    return port_;
  }

  /// Stops accepting, answers the calls in flight, and closes every
  /// connection. Handlers must finish within 10 s: fbthrift aborts the
  /// process if a call still runs then.
  void stop();

 private:
  // The running fbthrift server.
  struct Impl;

  std::unique_ptr<Impl> impl_;
  uint16_t port_{0};
};

} // namespace ember::rpc
