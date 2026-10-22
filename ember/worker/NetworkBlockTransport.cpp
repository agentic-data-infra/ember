#include "ember/worker/NetworkBlockTransport.h"

#include "ember/rpc/gen-cpp2/ShuffleBlockServiceAsyncClient.h"
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
  std::shared_ptr<Client> connection;
  try {
    connection = client(location);
  } catch (...) {
    return folly::makeSemiFuture<std::unique_ptr<folly::IOBuf>>(
        folly::exception_wrapper(std::current_exception()));
  }
  rpc::thrift::BlockId block;
  block.shuffleId() = id.shuffleId;
  block.mapId() = id.mapId;
  block.reduceId() = id.reduceId;
  return connection
      ->call([&](auto& c) { return c.semifuture_fetchBlock(block); })
      .within(
          fetchTimeout_,
          std::runtime_error(fmt::format(
              "No reply from {} within {} ms",
              location,
              fetchTimeout_.count())))
      .deferValue([](folly::IOBuf bytes) {
        return std::make_unique<folly::IOBuf>(std::move(bytes));
      });
}

std::shared_ptr<NetworkBlockTransport::Client> NetworkBlockTransport::client(
    const std::string& location) {
  std::lock_guard<std::mutex> l(mutex_);
  auto& client = clients_[location];
  if (client == nullptr || !client->isOpen()) {
    client = Client::connect(rpc::HostAndPort::parse(location));
  }
  return client;
}

folly::SemiFuture<std::unique_ptr<folly::IOBuf>>
ShuffleBlockHandler::semifuture_fetchBlock(
    std::unique_ptr<rpc::thrift::BlockId> block) {
  return folly::via(
             executor_,
             [getStore = getStore_,
              id = shuffle::BlockId{
                  *block->shuffleId(), *block->mapId(), *block->reduceId()}]()
                 -> std::unique_ptr<folly::IOBuf> {
               auto store = getStore();
               VELOX_CHECK_NOT_NULL(
                   store, "No executor serves shuffle blocks here");
               return store->readBlock(id);
             })
      .semi();
}

} // namespace ember
