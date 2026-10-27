#include "ember/shuffle/KilnShuffle.h"

#include <folly/coro/BlockingWait.h>

#include "kiln/client/Client.h"
#include "velox/common/base/Exceptions.h"

namespace ember::shuffle {

namespace {

using folly::coro::blockingWait;

// Bytes a writer buffers before appending them to its chunk.
constexpr size_t kAppendBytes = 4 << 20;

class KilnShuffleWriter : public PartitionedPageWriter {
 public:
  KilnShuffleWriter(
      std::shared_ptr<ShuffleBlockStore> store,
      std::string metadata,
      int32_t shuffleId,
      int32_t mapId,
      int32_t numPartitions,
      std::string attemptTag,
      int64_t spillThreshold)
      : PartitionedPageWriter(
            std::move(store),
            shuffleId,
            mapId,
            numPartitions,
            std::move(attemptTag),
            spillThreshold),
        metadata_{std::move(metadata)} {}

 protected:
  MapStatus commitOutput() override {
    auto client = kilnClient(metadata_);
    auto chunk = blockingWait(client->co_create());
    std::vector<int64_t> offsets;
    try {
      auto buffer = client->allocateBuffer(kAppendBytes);
      auto flush = [&]() {
        if (buffer.size() > 0) {
          buffer = blockingWait(chunk->co_append(std::move(buffer)));
          buffer.setSize(0);
        }
      };
      offsets = writePartitions([&](const void* data, size_t size) {
        auto* bytes = static_cast<const uint8_t*>(data);
        while (size > 0) {
          const auto n = std::min(size, buffer.capacity() - buffer.size());
          std::memcpy(buffer.data() + buffer.size(), bytes, n);
          buffer.setSize(buffer.size() + n);
          bytes += n;
          size -= n;
          if (buffer.size() == buffer.capacity()) {
            flush();
          }
        }
      });
      flush();
    } catch (const std::exception&) {
      blockingWait(chunk->co_abort());
      throw;
    }

    MapStatus status;
    if (offsets.back() == 0) {
      // No chunk for an empty output: it has no blocks to read.
      blockingWait(chunk->co_abort());
      status.location = kilnLocation(metadata_, "");
    } else {
      const auto id = blockingWait(chunk->co_commit());
      status.location = kilnLocation(metadata_, id.toString());
    }
    for (size_t i = 1; i < offsets.size(); ++i) {
      status.partitionLengths.push_back(offsets[i] - offsets[i - 1]);
    }
    store().commitKilnOutput(shuffleId(), mapId(), std::move(status));
    return store().mapStatus(shuffleId(), mapId());
  }

 private:
  const std::string metadata_;
};

} // namespace

// One client per cluster: clients cache where chunks are and keep their
// connections.
std::shared_ptr<kiln::client::Client> kilnClient(const std::string& metadata) {
  static std::mutex mutex;
  static std::map<std::string, std::shared_ptr<kiln::client::Client>> clients;
  std::lock_guard<std::mutex> l(mutex);
  auto& client = clients[metadata];
  if (client == nullptr) {
    client = std::make_shared<kiln::client::Client>(kiln::client::ClientOptions{
        .metadata = kiln::Endpoint::parse(metadata)});
  }
  return client;
}

std::pair<std::string, std::string> parseKilnLocation(
    const std::string& location) {
  VELOX_CHECK(isKilnLocation(location), "Not a Kiln location: {}", location);
  // kiln://<metadata>/<chunk>
  const auto path = location.substr(location.find("//") + 2);
  const auto slash = path.find('/');
  VELOX_CHECK_NE(slash, std::string::npos, "Malformed Kiln location: {}", location);
  return {path.substr(0, slash), path.substr(slash + 1)};
}

std::unique_ptr<ShuffleWriter> createKilnWriter(
    std::shared_ptr<ShuffleBlockStore> store,
    std::string metadata,
    int32_t shuffleId,
    int32_t mapId,
    int32_t numPartitions,
    const std::string& attemptTag,
    int64_t spillThreshold) {
  return std::make_unique<KilnShuffleWriter>(
      std::move(store),
      std::move(metadata),
      shuffleId,
      mapId,
      numPartitions,
      attemptTag,
      spillThreshold);
}

folly::coro::Task<std::unique_ptr<folly::IOBuf>> co_fetchKilnBlock(
    std::string url) {
  const auto [metadata, chunk] = parseKilnLocation(parseBlockUrl(url).first);
  auto client = kilnClient(metadata);
  const auto id = kiln::ChunkId::parse(chunk);
  const auto [offset, length] = kilnBlockRange(url);
  // Kiln reads straight into the block's IOBuf.
  auto data = folly::IOBuf::create(length);
  auto buffer = client->registerMemory(data->writableData(), length);
  auto read = co_await client->co_read(id, offset, length, std::move(buffer));
  VELOX_CHECK_EQ(
      read.size(), length, "Short read of Kiln chunk {}", id.toString());
  data->append(length);
  co_return data;
}

} // namespace ember::shuffle
