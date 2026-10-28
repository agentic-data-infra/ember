#include "ember/rpc/Clients.h"

#include "ember/rpc/ThriftConnection.h"
#include "ember/rpc/gen-cpp2/DriverServiceAsyncClient.h"
#include "ember/rpc/gen-cpp2/ShuffleBlockServiceAsyncClient.h"
#include "ember/rpc/gen-cpp2/WorkerServiceAsyncClient.h"

namespace ember::rpc {

Client::Client(const HostAndPort& address, std::chrono::milliseconds timeout)
    : connection_{std::make_unique<detail::Connection>(address, timeout)} {}

Client::~Client() = default;

const HostAndPort& Client::address() const {
  return connection_->address();
}

bool Client::isOpen() const {
  return connection_->isOpen();
}

void Client::close() {
  connection_->close();
}

// The generated client of a service over a connection.
template <typename Service>
struct GeneratedClient {
  explicit GeneratedClient(const detail::Connection& connection)
      : connection{connection},
        client{detail::makeClient<Service>(connection)} {}

  ~GeneratedClient() {
    detail::destroyClient(connection, client);
  }

  const detail::Connection& connection;
  std::unique_ptr<apache::thrift::Client<Service>> client;
};

struct DriverClient::Stub : GeneratedClient<thrift::DriverService> {
  using GeneratedClient::GeneratedClient;
};

DriverClient::DriverClient(
    const HostAndPort& address,
    std::chrono::milliseconds timeout)
    : Client(address, timeout),
      stub_{std::make_unique<Stub>(*connection_)} {}

DriverClient::~DriverClient() = default;

std::shared_ptr<DriverClient> DriverClient::connect(
    const HostAndPort& address,
    std::chrono::milliseconds timeout) {
  return std::shared_ptr<DriverClient>(new DriverClient(address, timeout));
}

folly::coro::Task<RegisterWorkerResponse> DriverClient::co_registerWorker(
    RegisterWorkerRequest request) {
  thrift::RegisterWorkerRequest message;
  message.workerId() = std::move(request.workerId);
  message.address() = std::move(request.address);
  message.slots() = request.slots;
  message.node() = std::move(request.node);
  auto response = co_await detail::call(
      *connection_, stub_->client->co_registerWorker(message));
  co_return RegisterWorkerResponse{
      .appName = std::move(*response.appName()),
      .heartbeatIntervalMs = *response.heartbeatIntervalMs()};
}

folly::coro::Task<bool> DriverClient::co_heartbeat(std::string workerId) {
  thrift::HeartbeatRequest message;
  message.workerId() = std::move(workerId);
  auto response =
      co_await detail::call(*connection_, stub_->client->co_heartbeat(message));
  co_return *response.registered();
}

struct WorkerClient::Stub : GeneratedClient<thrift::WorkerService> {
  using GeneratedClient::GeneratedClient;
};

WorkerClient::WorkerClient(
    const HostAndPort& address,
    std::chrono::milliseconds timeout)
    : Client(address, timeout),
      stub_{std::make_unique<Stub>(*connection_)} {}

WorkerClient::~WorkerClient() = default;

std::shared_ptr<WorkerClient> WorkerClient::connect(
    const HostAndPort& address,
    std::chrono::milliseconds timeout) {
  return std::shared_ptr<WorkerClient>(new WorkerClient(address, timeout));
}

folly::coro::Task<std::string> WorkerClient::co_launchTask(std::string task) {
  co_return co_await detail::call(
      *connection_, stub_->client->co_launchTask(task));
}

folly::coro::Task<void> WorkerClient::co_removeShuffle(int32_t shuffleId) {
  co_await detail::call(
      *connection_, stub_->client->co_removeShuffle(shuffleId));
}

folly::coro::Task<void> WorkerClient::co_shutdown() {
  co_await detail::call(*connection_, stub_->client->co_shutdown());
}

folly::coro::Task<std::vector<FileInfo>> WorkerClient::co_listFiles(
    std::string schema,
    std::string path) {
  auto files = co_await detail::call(
      *connection_, stub_->client->co_listFiles(schema, path));
  std::vector<FileInfo> result;
  for (auto& file : files) {
    result.push_back(
        FileInfo{.path = std::move(*file.path()), .size = *file.size()});
  }
  co_return result;
}

struct ShuffleBlockClient::Stub
    : GeneratedClient<thrift::ShuffleBlockService> {
  using GeneratedClient::GeneratedClient;
};

ShuffleBlockClient::ShuffleBlockClient(
    const HostAndPort& address,
    std::chrono::milliseconds timeout)
    : Client(address, timeout),
      stub_{std::make_unique<Stub>(*connection_)} {}

ShuffleBlockClient::~ShuffleBlockClient() = default;

std::shared_ptr<ShuffleBlockClient> ShuffleBlockClient::connect(
    const HostAndPort& address,
    std::chrono::milliseconds timeout) {
  return std::shared_ptr<ShuffleBlockClient>(
      new ShuffleBlockClient(address, timeout));
}

folly::coro::Task<std::unique_ptr<folly::IOBuf>>
ShuffleBlockClient::co_fetchBlock(BlockId block) {
  thrift::BlockId message;
  message.shuffleId() = block.shuffleId;
  message.mapId() = block.mapId;
  message.reduceId() = block.reduceId;
  auto bytes = co_await detail::call(
      *connection_, stub_->client->co_fetchBlock(message));
  co_return std::make_unique<folly::IOBuf>(std::move(bytes));
}

} // namespace ember::rpc
