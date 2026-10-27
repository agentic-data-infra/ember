#include "ember/shuffle/KilnShuffle.h"

#include <folly/ScopeGuard.h>
#include <folly/Uri.h>
#include <folly/coro/BlockingWait.h>

#include "kiln/client/Client.h"
#include "velox/common/base/Exceptions.h"

namespace ember::shuffle {

namespace {

using folly::coro::blockingWait;

// Bytes a writer buffers before appending them to its chunk, and the size
// of the pooled buffers.
constexpr size_t kAppendBytes = 4 << 20;

// Pooled buffers in use at once per cluster.
constexpr size_t kPoolBuffers = 16;

// Buffers from the client's allocateBuffer, kept for reuse: with RDMA each
// one is registered with the NIC, which costs far more than moving a block.
// At most kPoolBuffers are in use at once; when all are, callers take
// unpooled memory as before.
class BufferPool : public std::enable_shared_from_this<BufferPool> {
 public:
  explicit BufferPool(std::shared_ptr<kiln::client::Client> client)
      : client_{std::move(client)} {}

  /// An empty buffer of kAppendBytes, or none if all are in use. Give it
  /// back with release().
  std::optional<kiln::Buffer> acquire() {
    {
      std::lock_guard<std::mutex> l(mutex_);
      if (inUse_ == kPoolBuffers) {
        return std::nullopt;
      }
      ++inUse_;
      if (!free_.empty()) {
        auto buffer = std::move(free_.back());
        free_.pop_back();
        buffer.setSize(0);
        return buffer;
      }
    }
    try {
      return client_->allocateBuffer(kAppendBytes);
    } catch (...) {
      release({});
      throw;
    }
  }

  /// Returns a buffer from acquire(). An empty one, e.g. lost to a failed
  /// call, just frees its place.
  void release(kiln::Buffer buffer) {
    std::lock_guard<std::mutex> l(mutex_);
    --inUse_;
    if (buffer.capacity() == kAppendBytes) {
      free_.push_back(std::move(buffer));
    }
  }

  /// The data of 'buffer', from acquire(), as an IOBuf that returns the
  /// buffer when freed.
  std::unique_ptr<folly::IOBuf> wrap(kiln::Buffer buffer) {
    auto* held = new Held{shared_from_this(), std::move(buffer)};
    return folly::IOBuf::takeOwnership(
        held->buffer.data(),
        held->buffer.capacity(),
        held->buffer.size(),
        [](void* /*data*/, void* userData) {
          auto* held = static_cast<Held*>(userData);
          held->pool->release(std::move(held->buffer));
          delete held;
        },
        held);
  }

 private:
  struct Held {
    std::shared_ptr<BufferPool> pool;
    kiln::Buffer buffer;
  };

  const std::shared_ptr<kiln::client::Client> client_;
  std::mutex mutex_;
  std::vector<kiln::Buffer> free_;
  size_t inUse_{0};
};

// The process's client of a Kiln cluster and its buffer pool.
struct KilnCluster {
  std::shared_ptr<kiln::client::Client> client;
  std::shared_ptr<BufferPool> pool;
};

// One client per cluster: clients cache where chunks are and keep their
// connections.
KilnCluster kilnCluster(const std::string& metadata) {
  static std::mutex mutex;
  static std::map<std::string, KilnCluster> clusters;
  std::lock_guard<std::mutex> l(mutex);
  auto& cluster = clusters[metadata];
  if (cluster.client == nullptr) {
    cluster.client =
        std::make_shared<kiln::client::Client>(kiln::client::ClientOptions{
            .metadata = kiln::MetadataAddress::parse(metadata)});
    cluster.pool = std::make_shared<BufferPool>(cluster.client);
  }
  return cluster;
}

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
    const auto cluster = kilnCluster(metadata_);
    const auto& client = cluster.client;
    const auto& pool = cluster.pool;
    auto chunk = blockingWait(client->co_create());
    std::vector<int64_t> offsets;
    try {
      auto pooled = pool->acquire();
      auto buffer = pooled.has_value() ? std::move(*pooled)
                                       : client->allocateBuffer(kAppendBytes);
      SCOPE_EXIT {
        if (pooled.has_value()) {
          pool->release(std::move(buffer));
        }
      };
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

std::shared_ptr<kiln::client::Client> kilnClient(const std::string& metadata) {
  return kilnCluster(metadata).client;
}

std::pair<std::string, std::string> parseKilnLocation(
    const std::string& location) {
  VELOX_CHECK(isKilnLocation(location), "Not a Kiln location: {}", location);
  // kiln://<metadata>/<chunk>
  const auto path = location.substr(location.find("//") + 2);
  const auto slash = path.find('/');
  VELOX_CHECK_NE(slash, std::string::npos, "Malformed Kiln location: {}", location);
  return {
      folly::uriUnescape<std::string>(path.substr(0, slash)),
      path.substr(slash + 1)};
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
  const auto cluster = kilnCluster(metadata);
  const auto& client = cluster.client;
  const auto id = kiln::ChunkId::parse(chunk);
  const auto [offset, length] = kilnBlockRange(url);
  // A block that fits goes into a pooled buffer, which Velox then holds as
  // the block's IOBuf.
  auto pooled = length <= static_cast<int64_t>(kAppendBytes)
      ? cluster.pool->acquire()
      : std::nullopt;
  if (pooled.has_value()) {
    auto result = co_await folly::coro::co_awaitTry(
        client->co_read(id, offset, length, std::move(*pooled)));
    if (result.hasException()) {
      cluster.pool->release(std::move(*pooled));
      co_yield folly::coro::co_error(std::move(result.exception()));
    }
    auto read = std::move(result).value();
    if (read.size() != static_cast<size_t>(length)) {
      cluster.pool->release(std::move(read));
      VELOX_FAIL("Short read of Kiln chunk {}", id.toString());
    }
    co_return cluster.pool->wrap(std::move(read));
  }
  // Else Kiln reads straight into the block's IOBuf.
  auto data = folly::IOBuf::create(length);
  auto buffer = client->registerMemory(data->writableData(), length);
  auto read = co_await client->co_read(id, offset, length, std::move(buffer));
  VELOX_CHECK_EQ(
      read.size(), length, "Short read of Kiln chunk {}", id.toString());
  data->append(length);
  co_return data;
}

} // namespace ember::shuffle
