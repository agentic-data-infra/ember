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

folly::SemiFuture<RegisterWorkerResponse> DriverClient::registerWorker(
    const RegisterWorkerRequest& request) {
  thrift::RegisterWorkerRequest message;
  message.workerId() = request.workerId;
  message.address() = request.address;
  message.slots() = request.slots;
  return detail::call(
             *connection_,
             [&] { return stub_->client->semifuture_registerWorker(message); })
      .deferValue([](thrift::RegisterWorkerResponse response) {
        return RegisterWorkerResponse{
            .appName = std::move(*response.appName()),
            .heartbeatIntervalMs = *response.heartbeatIntervalMs()};
      });
}

folly::SemiFuture<bool> DriverClient::heartbeat(const std::string& workerId) {
  thrift::HeartbeatRequest message;
  message.workerId() = workerId;
  return detail::call(
             *connection_,
             [&] { return stub_->client->semifuture_heartbeat(message); })
      .deferValue([](thrift::HeartbeatResponse response) {
        return *response.registered();
      });
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

folly::SemiFuture<std::string> WorkerClient::launchTask(
    const std::string& task) {
  return detail::call(
      *connection_, [&] { return stub_->client->semifuture_launchTask(task); });
}

folly::SemiFuture<folly::Unit> WorkerClient::removeShuffle(int32_t shuffleId) {
  return detail::call(*connection_, [&] {
    return stub_->client->semifuture_removeShuffle(shuffleId);
  });
}

folly::SemiFuture<folly::Unit> WorkerClient::shutdown() {
  return detail::call(
      *connection_, [&] { return stub_->client->semifuture_shutdown(); });
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

folly::SemiFuture<std::unique_ptr<folly::IOBuf>> ShuffleBlockClient::fetchBlock(
    const BlockId& block) {
  thrift::BlockId message;
  message.shuffleId() = block.shuffleId;
  message.mapId() = block.mapId;
  message.reduceId() = block.reduceId;
  return detail::call(
             *connection_,
             [&] { return stub_->client->semifuture_fetchBlock(message); })
      .deferValue([](folly::IOBuf bytes) {
        return std::make_unique<folly::IOBuf>(std::move(bytes));
      });
}

} // namespace ember::rpc
