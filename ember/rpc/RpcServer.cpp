#include "ember/rpc/RpcServer.h"

#include <fmt/format.h>
#include <thrift/lib/cpp2/server/ThriftServer.h>
#include <thrift/lib/cpp2/util/ScopedServerInterfaceThread.h>

#include "ember/rpc/gen-cpp2/DriverService.h"
#include "ember/rpc/gen-cpp2/WorkerService.h"

namespace ember::rpc {

namespace {

// The generated services, answered by Ember's handlers.

class DriverService
    : public apache::thrift::ServiceHandler<thrift::DriverService> {
 public:
  explicit DriverService(std::shared_ptr<DriverHandler> handler)
      : handler_{std::move(handler)} {}

  void sync_registerWorker(
      thrift::RegisterWorkerResponse& response,
      std::unique_ptr<thrift::RegisterWorkerRequest> request) override {
    auto result = handler_->registerWorker(RegisterWorkerRequest{
        .workerId = std::move(*request->workerId()),
        .address = std::move(*request->address()),
        .slots = *request->slots()});
    response.appName() = std::move(result.appName);
    response.heartbeatIntervalMs() = result.heartbeatIntervalMs;
  }

  void sync_heartbeat(
      thrift::HeartbeatResponse& response,
      std::unique_ptr<thrift::HeartbeatRequest> request) override {
    response.registered() = handler_->heartbeat(*request->workerId());
  }

 private:
  const std::shared_ptr<DriverHandler> handler_;
};

class WorkerService
    : public apache::thrift::ServiceHandler<thrift::WorkerService> {
 public:
  explicit WorkerService(std::shared_ptr<WorkerHandler> handler)
      : handler_{std::move(handler)} {}

  folly::SemiFuture<std::unique_ptr<std::string>> semifuture_launchTask(
      std::unique_ptr<std::string> task) override {
    return folly::makeSemiFutureWith(
               [&] { return handler_->launchTask(std::move(*task)); })
        .deferValue([](std::string result) {
          return std::make_unique<std::string>(std::move(result));
        });
  }

  void sync_removeShuffle(int32_t shuffleId) override {
    handler_->removeShuffle(shuffleId);
  }

  void sync_shutdown() override {
    handler_->shutdown();
  }

  folly::SemiFuture<std::unique_ptr<folly::IOBuf>> semifuture_fetchBlock(
      std::unique_ptr<thrift::BlockId> block) override {
    return folly::makeSemiFutureWith([&] {
      return handler_->fetchBlock(BlockId{
          .shuffleId = *block->shuffleId(),
          .mapId = *block->mapId(),
          .reduceId = *block->reduceId()});
    });
  }

 private:
  const std::shared_ptr<WorkerHandler> handler_;
};

} // namespace

struct RpcServer::Impl {
  std::unique_ptr<apache::thrift::ScopedServerInterfaceThread> server;
};

namespace {

std::unique_ptr<apache::thrift::ScopedServerInterfaceThread> serve(
    std::shared_ptr<apache::thrift::AsyncProcessorFactory> service,
    const std::string& host,
    uint16_t port) {
  // "::" accepts IPv4 connections too.
  const auto bindHost = host.empty() ? std::string("::") : host;
  try {
    return std::make_unique<apache::thrift::ScopedServerInterfaceThread>(
        std::move(service),
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
}

} // namespace

RpcServer::RpcServer() = default;

RpcServer::~RpcServer() {
  stop();
}

void RpcServer::start(
    std::shared_ptr<DriverHandler> handler,
    const std::string& host,
    uint16_t port) {
  impl_ = std::make_unique<Impl>(
      serve(std::make_shared<DriverService>(std::move(handler)), host, port));
  port_ = impl_->server->getPort();
}

void RpcServer::start(
    std::shared_ptr<WorkerHandler> handler,
    const std::string& host,
    uint16_t port) {
  impl_ = std::make_unique<Impl>(
      serve(std::make_shared<WorkerService>(std::move(handler)), host, port));
  port_ = impl_->server->getPort();
}

void RpcServer::stop() {
  impl_.reset();
}

} // namespace ember::rpc
