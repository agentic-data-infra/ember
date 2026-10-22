// Fetches shuffle blocks from other workers over RPC
// (ShuffleBlockService.fetchBlock in ember.thrift).
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

#include "ember/rpc/RpcClient.h"
#include "ember/rpc/gen-cpp2/ShuffleBlockService.h"
#include "ember/shuffle/FileShuffle.h"

namespace ember {

class NetworkBlockTransport : public shuffle::BlockTransport {
 public:
  explicit NetworkBlockTransport(
      bool fetchLocalBlocks = true,
      std::chrono::milliseconds fetchTimeout = std::chrono::seconds(120))
      : fetchLocalBlocks_{fetchLocalBlocks}, fetchTimeout_{fetchTimeout} {}

  folly::SemiFuture<std::unique_ptr<folly::IOBuf>> fetchBlock(
      const std::string& location,
      const shuffle::BlockId& id) override;

  /// Fetches that went over the network.
  int64_t remoteFetches() const {
    return remoteFetches_;
  }

 private:
  using Client = rpc::RpcClient<rpc::thrift::ShuffleBlockService>;

  std::shared_ptr<Client> client(const std::string& location);

  const bool fetchLocalBlocks_;
  const std::chrono::milliseconds fetchTimeout_;
  std::mutex mutex_;
  std::map<std::string, std::shared_ptr<Client>> clients_;
  std::atomic<int64_t> remoteFetches_{0};
};

/// Serves fetchBlock from the store 'getStore' returns at the time of the
/// request, reading blocks on 'executor'. A worker's handler derives from it.
class ShuffleBlockHandler
    : virtual public apache::thrift::ServiceHandler<
          rpc::thrift::ShuffleBlockService> {
 public:
  ShuffleBlockHandler(
      std::function<std::shared_ptr<shuffle::ShuffleBlockStore>()> getStore,
      folly::Executor* executor)
      : getStore_{std::move(getStore)}, executor_{executor} {}

  folly::SemiFuture<std::unique_ptr<folly::IOBuf>> semifuture_fetchBlock(
      std::unique_ptr<rpc::thrift::BlockId> block) override;

 private:
  const std::function<std::shared_ptr<shuffle::ShuffleBlockStore>()>
      getStore_;
  folly::Executor* const executor_;
};

} // namespace ember
