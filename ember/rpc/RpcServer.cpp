#include "ember/rpc/RpcServer.h"

#include <atomic>
#include <future>
#include <thread>

#include <fmt/format.h>
#include <folly/ExceptionString.h>
#include <folly/SocketAddress.h>
#include <glog/logging.h>
#include <thrift/lib/cpp/server/TServerEventHandler.h>
#include <thrift/lib/cpp2/server/ThriftServer.h>

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

constexpr int kIoThreads = 4;
constexpr int kHandlerThreads = 4;
constexpr auto kStopTimeout = std::chrono::seconds(10);

// Tells ServingThread once its server listens, or why it cannot.
class Started : public apache::thrift::server::TServerEventHandler {
 public:
  void preServe(const folly::SocketAddress* address) override {
    port_ = address->getPort();
    done_ = true;
    promise_.set_value();
  }

  void failed(std::exception_ptr error) {
    if (done_.exchange(true)) {
      LOG(ERROR) << "RPC server failed: "
                 << folly::exceptionStr(error).toStdString();
      return;
    }
    promise_.set_exception(std::move(error));
  }

  // The port served; throws why the server could not start.
  uint16_t wait() {
    promise_.get_future().get();
    return port_;
  }

 private:
  std::promise<void> promise_;
  std::atomic<bool> done_{false};
  uint16_t port_{0};
};

// A ThriftServer serving on its own thread until destroyed.
class ServingThread {
 public:
  ServingThread(
      std::shared_ptr<apache::thrift::AsyncProcessorFactory> service,
      const std::string& host,
      uint16_t port) {
    // "::" accepts IPv4 connections too.
    const auto bindHost = host.empty() ? std::string("::") : host;
    server_->setInterface(std::move(service));
    // Ember's RPC is plaintext.
    server_->setSSLPolicy(apache::thrift::SSLPolicy::DISABLED);
    server_->setNumIOWorkerThreads(kIoThreads);
    server_->setNumCPUWorkerThreads(kHandlerThreads);
    // Calls wait for a handler thread as long as it takes, rather than
    // failing after fbthrift's 100 ms queue timeout, which the driver would
    // take for a lost worker.
    server_->setQueueTimeout(std::chrono::milliseconds(0));
    // Connections stay up between jobs; a broken one means a lost peer.
    server_->setIdleTimeout(std::chrono::milliseconds(0));
    // Stopping waits this long for calls in flight, e.g. a peer's block
    // fetch; fbthrift aborts the process if one still runs then.
    server_->setWorkersJoinTimeout(kStopTimeout);
    auto started = std::make_shared<Started>();
    server_->addServerEventHandler(started);
    try {
      server_->setAddress(folly::SocketAddress(bindHost, port));
      thread_ = std::thread([server = server_, started] {
        try {
          server->serve();
        } catch (...) {
          started->failed(std::current_exception());
        }
      });
      port_ = started->wait();
    } catch (const std::exception& e) {
      stop();
      throw RpcError(fmt::format(
          "Cannot serve on {}: {}",
          HostAndPort{bindHost, port}.toString(),
          e.what()));
    }
  }

  ~ServingThread() {
    stop();
  }

  uint16_t port() const {
    return port_;
  }

 private:
  void stop() {
    if (thread_.joinable()) {
      server_->stop();
      thread_.join();
    }
  }

  const std::shared_ptr<apache::thrift::ThriftServer> server_{
      std::make_shared<apache::thrift::ThriftServer>()};
  std::thread thread_;
  uint16_t port_{0};
};

} // namespace

struct RpcServer::Impl : ServingThread {
  using ServingThread::ServingThread;
};

RpcServer::RpcServer() = default;

RpcServer::~RpcServer() {
  stop();
}

void RpcServer::start(
    std::shared_ptr<DriverHandler> handler,
    const std::string& host,
    uint16_t port) {
  impl_ = std::make_unique<Impl>(
      std::make_shared<DriverService>(std::move(handler)), host, port);
  port_ = impl_->port();
}

void RpcServer::start(
    std::shared_ptr<WorkerHandler> handler,
    const std::string& host,
    uint16_t port) {
  impl_ = std::make_unique<Impl>(
      std::make_shared<WorkerService>(std::move(handler)), host, port);
  port_ = impl_->port();
}

void RpcServer::stop() {
  impl_.reset();
}

} // namespace ember::rpc
