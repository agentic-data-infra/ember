// The serving side: one of Ember's Thrift services (ember.thrift) on a port.
//
// A handler implements the generated apache::thrift::ServiceHandler<Service>,
// typically its semifuture_ methods. They are called on the server's own
// threads; one that blocks or runs long (a task, a disk read) should move its
// work to an executor and return the future. A handler that throws, or whose
// future fails, fails the call with the exception's message.

#pragma once

#include <memory>
#include <string>

#include "ember/rpc/Rpc.h"

namespace apache::thrift {
class AsyncProcessorFactory;
class ScopedServerInterfaceThread;
} // namespace apache::thrift

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
      std::shared_ptr<apache::thrift::AsyncProcessorFactory> handler,
      const std::string& host,
      uint16_t port);

  /// The port served, once started.
  uint16_t port() const {
    return port_;
  }

  /// Stops accepting and closes every connection. Calls still running are
  /// not answered.
  void stop();

 private:
  std::unique_ptr<apache::thrift::ScopedServerInterfaceThread> server_;
  uint16_t port_{0};
};

} // namespace ember::rpc
