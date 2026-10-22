#include "ember/rpc/RpcServer.h"

#include <fmt/format.h>
#include <thrift/lib/cpp2/server/ThriftServer.h>
#include <thrift/lib/cpp2/util/ScopedServerInterfaceThread.h>

namespace ember::rpc {

RpcServer::RpcServer() = default;

RpcServer::~RpcServer() {
  stop();
}

void RpcServer::start(
    std::shared_ptr<apache::thrift::AsyncProcessorFactory> handler,
    const std::string& host,
    uint16_t port) {
  // "::" accepts IPv4 connections too.
  const auto bindHost = host.empty() ? std::string("::") : host;
  try {
    server_ = std::make_unique<apache::thrift::ScopedServerInterfaceThread>(
        std::move(handler),
        bindHost,
        port,
        [](apache::thrift::ThriftServer& server) {
          // Connections stay up between jobs; a broken one means a lost
          // peer.
          server.setIdleTimeout(std::chrono::milliseconds(0));
          // Stopping does not wait for running tasks.
          server.setWorkersJoinTimeout(std::chrono::seconds(1));
        });
  } catch (const std::exception& e) {
    throw RpcError(fmt::format(
        "Cannot serve on {}: {}",
        HostAndPort{bindHost, port}.toString(),
        e.what()));
  }
  port_ = server_->getPort();
}

void RpcServer::stop() {
  server_.reset();
}

} // namespace ember::rpc
