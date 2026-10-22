#include "ember/worker/NetworkBlockTransport.h"

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
  std::shared_ptr<rpc::ShuffleBlockClient> connection;
  try {
    connection = client(location);
  } catch (...) {
    return folly::makeSemiFuture<std::unique_ptr<folly::IOBuf>>(
        folly::exception_wrapper(std::current_exception()));
  }
  return connection
      ->fetchBlock(rpc::BlockId{
          .shuffleId = id.shuffleId, .mapId = id.mapId, .reduceId = id.reduceId})
      .within(
          fetchTimeout_,
          std::runtime_error(fmt::format(
              "No reply from {} within {} ms",
              location,
              fetchTimeout_.count())));
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
