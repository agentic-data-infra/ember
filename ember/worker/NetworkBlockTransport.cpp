#include "ember/worker/NetworkBlockTransport.h"

#include <folly/coro/Timeout.h>

#include "velox/common/base/Exceptions.h"

namespace ember {

folly::coro::Task<std::unique_ptr<folly::IOBuf>>
NetworkBlockTransport::co_fetchBlock(
    std::string location,
    shuffle::BlockId id) {
  if (fetchLocalBlocks_ && shuffle::ShuffleBlockStore::find(location)) {
    shuffle::LocalBlockTransport local;
    co_return co_await local.co_fetchBlock(std::move(location), id);
  }
  ++remoteFetches_;
  auto connection = client(location);
  const rpc::BlockId block{
      .shuffleId = id.shuffleId, .mapId = id.mapId, .reduceId = id.reduceId};
  // A fetch that times out is cancelled.
  auto result = co_await folly::coro::co_awaitTry(
      folly::coro::timeout(connection->co_fetchBlock(block), fetchTimeout_));
  if (result.hasException<folly::FutureTimeout>()) {
    throw std::runtime_error(fmt::format(
        "No reply from {} within {} ms", location, fetchTimeout_.count()));
  }
  co_return std::move(result).value();
}

std::shared_ptr<rpc::ShuffleBlockClient> NetworkBlockTransport::client(
    const std::string& location) {
  std::lock_guard<std::mutex> l(mutex_);
  auto& client = clients_[location];
  if (client == nullptr || !client->isOpen()) {
    client = rpc::ShuffleBlockClient::connect(rpc::HostAndPort::parse(location));
  }
  return client;
}

} // namespace ember
