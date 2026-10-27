// Fetches shuffle blocks from other workers over RPC
// (rpc::ShuffleBlockClient).
//
// A block's location is the "host:port" of the worker that wrote it. Blocks
// held by an executor in this process are read from its store directly,
// unless 'fetchLocalBlocks' is off, which sends every fetch over TCP (used by
// tests). One connection per remote worker carries all fetches to it. A fetch
// that takes longer than 'fetchTimeout' fails, so a reduce task does not wait
// for ever on a worker that hangs.

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
      bool fetchLocalBlocks = true,
      std::chrono::milliseconds fetchTimeout = std::chrono::seconds(120))
      : fetchLocalBlocks_{fetchLocalBlocks}, fetchTimeout_{fetchTimeout} {}

  folly::coro::Task<std::unique_ptr<folly::IOBuf>> co_fetchBlock(
      std::string location,
      shuffle::BlockId id) override;

  /// Fetches that went over the network.
  int64_t remoteFetches() const {
    return remoteFetches_;
  }

 private:
  std::shared_ptr<rpc::ShuffleBlockClient> client(const std::string& location);

  const bool fetchLocalBlocks_;
  const std::chrono::milliseconds fetchTimeout_;
  std::mutex mutex_;
  std::map<std::string, std::shared_ptr<rpc::ShuffleBlockClient>> clients_;
  std::atomic<int64_t> remoteFetches_{0};
};

} // namespace ember
