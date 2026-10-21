// The serving side: accepts connections and answers their requests with the
// handler registered for each method.
//
// A handler gets the request payload and returns a future of the response
// payload. It is called on the connection's reader thread, so a handler that
// blocks or runs long (a task, a disk read) should move its work to an
// executor and return the future. Responses go out as they complete, in any
// order.

#pragma once

#include <atomic>
#include <functional>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

#include <folly/futures/Future.h>
#include <folly/io/IOBuf.h>

#include "ember/rpc/Rpc.h"

namespace ember::rpc {

class RpcServer {
 public:
  using Handler = std::function<folly::SemiFuture<std::unique_ptr<folly::IOBuf>>(
      std::string payload)>;

  RpcServer() = default;
  ~RpcServer();

  RpcServer(const RpcServer&) = delete;
  RpcServer& operator=(const RpcServer&) = delete;

  /// Registers the handler of 'method'. Call before start().
  void setHandler(Method method, Handler handler);

  /// Starts serving on 'host' and 'port'; port 0 picks a free one. Throws
  /// RpcError if it cannot listen.
  void start(const std::string& host, uint16_t port);

  /// The port served, once started.
  uint16_t port() const {
    return port_;
  }

  /// Stops accepting and closes every connection. Responses still pending are
  /// dropped.
  void stop();

 private:
  struct Connection;

  void acceptLoop();
  void serve(const std::shared_ptr<Connection>& connection);
  void reapFinished();

  std::map<Method, Handler> handlers_;
  int listenFd_{-1};
  uint16_t port_{0};
  std::atomic<bool> stopped_{false};
  std::thread acceptor_;
  std::mutex mutex_;
  std::list<std::shared_ptr<Connection>> connections_;
};

/// A handler's response from bytes.
inline folly::SemiFuture<std::unique_ptr<folly::IOBuf>> respond(
    std::string payload) {
  return folly::makeSemiFuture(folly::IOBuf::fromString(std::move(payload)));
}

} // namespace ember::rpc
