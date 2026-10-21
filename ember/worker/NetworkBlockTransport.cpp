#include "ember/worker/NetworkBlockTransport.h"

#include "ember/rpc/Messages.h"
#include "velox/common/base/Exceptions.h"

namespace ember {

folly::SemiFuture<std::unique_ptr<folly::IOBuf>>
NetworkBlockTransport::fetchBlock(
    const std::string& location,
    const shuffle::BlockId& id) {
  if (fetchLocalBlocks_ && shuffle::ShuffleBlockStore::find(location)) {
    return shuffle::LocalBlockTransport().fetchBlock(location, id);
  }
  ++remoteFetches_;
  std::shared_ptr<rpc::RpcClient> connection;
  try {
    connection = client(location);
  } catch (...) {
    return folly::makeSemiFuture<std::unique_ptr<folly::IOBuf>>(
        folly::exception_wrapper(std::current_exception()));
  }
  return connection
      ->call(
          rpc::Method::kFetchBlock,
          rpc::FetchBlock{id.shuffleId, id.mapId, id.reduceId}.serialize())
      .deferValue([](std::string bytes) {
        return folly::IOBuf::fromString(std::move(bytes));
      });
}

std::shared_ptr<rpc::RpcClient> NetworkBlockTransport::client(
    const std::string& location) {
  std::lock_guard<std::mutex> l(mutex_);
  auto& client = clients_[location];
  if (client == nullptr || !client->isOpen()) {
    client = rpc::RpcClient::connect(rpc::HostAndPort::parse(location));
  }
  return client;
}

void serveBlocks(
    rpc::RpcServer& server,
    std::function<std::shared_ptr<shuffle::ShuffleBlockStore>()> getStore,
    folly::Executor* executor) {
  server.setHandler(
      rpc::Method::kFetchBlock,
      [getStore = std::move(getStore), executor](std::string payload) {
        auto request = rpc::FetchBlock::deserialize(payload);
        return folly::via(
                   executor,
                   [getStore, request]() -> std::unique_ptr<folly::IOBuf> {
                     auto store = getStore();
                     VELOX_CHECK_NOT_NULL(
                         store, "No executor serves shuffle blocks here");
                     return store->readBlock(shuffle::BlockId{
                         request.shuffleId, request.mapId, request.reduceId});
                   })
            .semi();
      });
}

} // namespace ember
