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

  folly::coro::Task<std::unique_ptr<std::string>> co_launchTask(
      std::unique_ptr<std::string> task) override {
    co_return std::make_unique<std::string>(
        co_await handler_->co_launchTask(std::move(*task)));
  }

  void sync_removeShuffle(int32_t shuffleId) override {
    handler_->removeShuffle(shuffleId);
  }

  void sync_shutdown() override {
    handler_->shutdown();
  }

  folly::coro::Task<std::unique_ptr<folly::IOBuf>> co_fetchBlock(
      std::unique_ptr<thrift::BlockId> block) override {
    const BlockId id{
        .shuffleId = *block->shuffleId(),
        .mapId = *block->mapId(),
        .reduceId = *block->reduceId()};
    co_return co_await handler_->co_fetchBlock(id);
  }

 private:
  const std::shared_ptr<WorkerHandler> handler_;
};

} // namespace

struct RpcServer::Impl {
  std::unique_ptr<apache::thrift::ScopedServerInterfaceThread> server;
};

namespace {

constexpr int kIoThreads = 4;
constexpr int kHandlerThreads = 4;
constexpr auto kStopTimeout = std::chrono::seconds(10);

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
          // ScopedServerInterfaceThread is made for tests and gives the
          // server one IO and one handler thread. Then one blocked handler
          // stalls every other call, and stopping, which runs the services'
          // onStopRequested on the handler threads, never finishes.
          server.setNumIOWorkerThreads(kIoThreads);
          server.setNumCPUWorkerThreads(kHandlerThreads);
          // Calls wait for a handler thread as long as it takes, rather than
          // failing after fbthrift's 100 ms queue timeout, which the driver
          // would take for a lost worker.
          server.setQueueTimeout(std::chrono::milliseconds(0));
          // Connections stay up between jobs; a broken one means a lost
          // peer.
          server.setIdleTimeout(std::chrono::milliseconds(0));
          // Stopping waits this long for calls in flight, e.g. a peer's
          // block fetch; fbthrift aborts the process if one still runs then.
          server.setWorkersJoinTimeout(kStopTimeout);
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
