// Fetches shuffle blocks from other workers over RPC
// (rpc::ShuffleBlockClient).
//
// A block's location is the "host:port" of the worker that wrote it. One
// connection per remote worker carries all fetches to it. A fetch that takes
// longer than 'fetchTimeout' fails, so a reduce task does not wait for ever on
// a worker that hangs.

#pragma once

#include <chrono>
#include <map>
#include <memory>
#include <mutex>

#include "ember/rpc/Clients.h"
#include "ember/shuffle/FileShuffle.h"

namespace ember {

class NetworkBlockTransport : public shuffle::BlockTransport {
 public:
  explicit NetworkBlockTransport(
      std::chrono::milliseconds fetchTimeout = std::chrono::seconds(120))
      : fetchTimeout_{fetchTimeout} {}

  folly::coro::Task<std::unique_ptr<folly::IOBuf>> co_fetchBlock(
      std::string location,
      shuffle::BlockId id,
      int64_t offset,
      int64_t length) override;

  /// Fetches that went over the network.
  int64_t remoteFetches() const {
    return remoteFetches_;
  }

 private:
  std::shared_ptr<rpc::ShuffleBlockClient> client(const std::string& location);

  const std::chrono::milliseconds fetchTimeout_;
  std::mutex mutex_;
  std::map<std::string, std::shared_ptr<rpc::ShuffleBlockClient>> clients_;
  std::atomic<int64_t> remoteFetches_{0};
};

} // namespace ember
