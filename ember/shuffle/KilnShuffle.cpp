#include "ember/shuffle/KilnShuffle.h"

#include <array>
#include <atomic>

#include <folly/ScopeGuard.h>
#include <folly/Uri.h>
#include <folly/coro/BlockingWait.h>
#include <folly/coro/Collect.h>
#include <folly/executors/GlobalExecutor.h>
#include <glog/logging.h>

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

// See setKilnUseShm().
std::atomic<bool> useShm{true};

// One client per cluster: clients cache where chunks are and keep their
// connections. They are never destroyed: at exit, folly's singletons may be
// gone already, and closing an RDMA metadata client needs its timekeeper.
KilnCluster kilnCluster(const std::string& metadata) {
  static std::mutex mutex;
  static auto* clusters = new std::map<std::string, KilnCluster>();
  std::lock_guard<std::mutex> l(mutex);
  auto& cluster = (*clusters)[metadata];
  if (cluster.client == nullptr) {
    cluster.client =
        std::make_shared<kiln::client::Client>(kiln::client::ClientOptions{
            .metadata = kiln::MetadataAddress::parse(metadata),
            .useShm = useShm.load()});
    cluster.pool = std::make_shared<BufferPool>(cluster.client);
  }
  return cluster;
}

class KilnShuffleWriter : public PartitionedPageWriter {
 public:
  KilnShuffleWriter(
      std::shared_ptr<ShuffleBlockStore> store,
      std::string metadata,
      kiln::BucketId bucket,
      kiln::Placement placement,
      int32_t shuffleId,
      int32_t mapId,
      int32_t numPartitions,
      std::string attemptTag,
      int64_t spillThreshold,
      bool spill)
      : PartitionedPageWriter(
            std::move(store),
            shuffleId,
            mapId,
            numPartitions,
            std::move(attemptTag),
            spillThreshold),
        metadata_{std::move(metadata)},
        bucket_{bucket},
        placement_{placement},
        spill_{spill} {}

  ~KilnShuffleWriter() override {
    discardChunk();
  }

  void abort() override {
    PartitionedPageWriter::abort();
    discardChunk();
  }

 protected:
  // With spill off, appends the buffered pages to the chunk as a segment of
  // their own instead of writing a run file.
  void spill() override {
    if (spill_) {
      PartitionedPageWriter::spill();
      return;
    }
    appendSegment();
  }

  MapStatus commitOutput() override {
    if (chunk_ == nullptr) {
      return commitWhole();
    }
    MapStatus status;
    try {
      if (outputBytes() > 0) {
        appendSegment();
      }
      awaitAppend();
      const auto id = blockingWait(chunk_->co_commit());
      chunk_.reset();
      status.location = kilnLocation(metadata_, id.toString());
    } catch (const std::exception&) {
      discardChunk();
      throw;
    }
    status.segments = std::move(segments_);
    return record(std::move(status));
  }

 private:
  // Writes the whole output, partition by partition, as one chunk of one
  // segment.
  MapStatus commitWhole() {
    const auto cluster = kilnCluster(metadata_);
    const auto& client = cluster.client;
    const auto& pool = cluster.pool;
    // With the whole size known, an overflow placement puts all of the
    // output into one tier.
    auto chunk = blockingWait(client->co_create(bucket_, placement_, outputBytes()));
    std::vector<int64_t> offsets;
    try {
      // Two buffers: the map task fills one while Kiln appends the other.
      std::array<kiln::Buffer, 2> buffers;
      std::array<bool, 2> pooled{};
      for (size_t i = 0; i < buffers.size(); ++i) {
        auto buffer = pool->acquire();
        pooled[i] = buffer.has_value();
        buffers[i] = pooled[i] ? std::move(*buffer)
                               : client->allocateBuffer(kAppendBytes);
      }
      size_t current = 0;
      std::optional<folly::SemiFuture<kiln::Buffer>> append;
      // Waits for the append in flight, of the other buffer, and empties
      // that buffer for reuse.
      auto awaitAppend = [&]() {
        if (append.has_value()) {
          auto future = std::move(*append);
          append.reset();
          auto& other = buffers[current ^ 1];
          other = std::move(future).get();
          other.setSize(0);
        }
      };
      SCOPE_EXIT {
        // On failure, co_append still holds its buffer: wait for it first.
        try {
          awaitAppend();
        } catch (const std::exception&) {
        }
        for (size_t i = 0; i < buffers.size(); ++i) {
          if (pooled[i]) {
            pool->release(std::move(buffers[i]));
          }
        }
      };
      // Starts appending the current buffer without waiting (Kiln takes one
      // append per chunk at a time, so after the previous one) and goes on
      // with the other.
      auto flush = [&]() {
        if (buffers[current].size() > 0) {
          awaitAppend();
          append = folly::coro::co_withExecutor(
                       folly::getGlobalCPUExecutor(),
                       chunk->co_append(std::move(buffers[current])))
                       .start();
          current ^= 1;
        }
      };
      offsets = writePartitions([&](const void* data, size_t size) {
        auto* bytes = static_cast<const uint8_t*>(data);
        while (size > 0) {
          auto& buffer = buffers[current];
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
      awaitAppend();
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
    status.segments.push_back(partitionLengths(offsets));
    return record(std::move(status));
  }

  // Copies the buffered pages into one registered buffer and starts
  // appending it to the chunk, without waiting: the map task goes on while
  // Kiln moves the bytes. Kiln takes one append per chunk at a time, so this
  // first waits for the previous one, which also frees its buffer for reuse.
  void appendSegment() {
    awaitAppend();
    const auto client = kilnCluster(metadata_).client;
    const auto bytes = outputBytes();
    if (chunk_ == nullptr) {
      // The size of the whole output is not known yet: an overflow placement
      // picks the tier by the first segment.
      chunk_ = blockingWait(client->co_create(bucket_, placement_, bytes));
    }
    if (buffer_.capacity() < static_cast<size_t>(bytes)) {
      buffer_ = client->allocateBuffer(bytes);
    }
    buffer_.setSize(0);
    const auto offsets = writeBuffered([&](const void* data, size_t size) {
      std::memcpy(buffer_.data() + buffer_.size(), data, size);
      buffer_.setSize(buffer_.size() + size);
    });
    segments_.push_back(partitionLengths(offsets));
    // co_append holds on to buffer_ until it completes and then hands it
    // back; awaitAppend() and discardChunk() wait for that.
    append_ = folly::coro::co_withExecutor(
                  folly::getGlobalCPUExecutor(),
                  chunk_->co_append(std::move(buffer_)))
                  .start();
  }

  // Waits for the append in flight, if any, and takes its buffer back.
  void awaitAppend() {
    if (append_.has_value()) {
      auto append = std::move(*append_);
      append_.reset();
      buffer_ = std::move(append).get();
    }
  }

  // Drops a chunk that was started but not committed. Its append in flight
  // still uses it, so that one is waited for first.
  void discardChunk() {
    if (append_.has_value()) {
      try {
        std::move(*append_).get();
      } catch (const std::exception&) {
      }
      append_.reset();
    }
    if (chunk_ != nullptr) {
      try {
        blockingWait(chunk_->co_abort());
      } catch (const std::exception& e) {
        LOG(WARNING) << "Could not abort Kiln chunk: " << e.what();
      }
      chunk_.reset();
    }
  }

  MapStatus record(MapStatus status) {
    store().commitKilnOutput(shuffleId(), mapId(), std::move(status));
    return store().mapStatus(shuffleId(), mapId());
  }

  const std::string metadata_;
  const kiln::BucketId bucket_;
  const kiln::Placement placement_;
  const bool spill_;
  // With spill off: the chunk, started at the first append; the append in
  // flight; the buffer appends copy into while none is in flight; and the
  // partition lengths of each append.
  std::unique_ptr<kiln::client::ChunkWriter> chunk_;
  std::optional<folly::SemiFuture<kiln::Buffer>> append_;
  kiln::Buffer buffer_;
  std::vector<std::vector<int64_t>> segments_;
};

} // namespace

void setKilnUseShm(bool use) {
  useShm = use;
}

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
    const std::string& tier,
    uint64_t bucket,
    int32_t shuffleId,
    int32_t mapId,
    int32_t numPartitions,
    const std::string& attemptTag,
    int64_t spillThreshold,
    bool spill) {
  kiln::Placement placement;
  if (tier == "dram+localfs") {
    placement = {.tier = kiln::Tier::kDram, .overflow = kiln::Tier::kLocalFs};
  } else {
    VELOX_USER_CHECK(
        tier == "dram" || tier == "localfs",
        "Kiln shuffle tier must be dram, localfs or dram+localfs, not {}",
        tier);
    placement = {.tier = kiln::parseTier(tier)};
  }
  return std::make_unique<KilnShuffleWriter>(
      std::move(store),
      std::move(metadata),
      bucket,
      placement,
      shuffleId,
      mapId,
      numPartitions,
      attemptTag,
      spillThreshold,
      spill);
}

namespace {

// Reads bytes [offset, offset + length) of chunk 'id'.
folly::coro::Task<std::unique_ptr<folly::IOBuf>> co_readRange(
    KilnCluster cluster,
    kiln::ChunkId id,
    int64_t offset,
    int64_t length) {
  const auto& client = cluster.client;
  // A range that fits goes into a pooled buffer, which Velox then holds as
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
  // Else Kiln reads straight into the range's IOBuf.
  auto data = folly::IOBuf::create(length);
  auto buffer = client->registerMemory(data->writableData(), length);
  auto read = co_await client->co_read(id, offset, length, std::move(buffer));
  VELOX_CHECK_EQ(
      read.size(), length, "Short read of Kiln chunk {}", id.toString());
  data->append(length);
  co_return data;
}

} // namespace

folly::coro::Task<std::unique_ptr<folly::IOBuf>> co_fetchKilnBlock(
    std::string url) {
  const auto [metadata, chunk] = parseKilnLocation(parseBlockUrl(url).first);
  const auto cluster = kilnCluster(metadata);
  const auto id = kiln::ChunkId::parse(chunk);
  // One range per segment of the map output, each whole frames: read them
  // all at once and chain them in order.
  std::vector<folly::coro::Task<std::unique_ptr<folly::IOBuf>>> reads;
  for (const auto& [offset, length] : kilnBlockRanges(url)) {
    reads.push_back(co_readRange(cluster, id, offset, length));
  }
  auto ranges = co_await folly::coro::collectAllRange(std::move(reads));
  std::unique_ptr<folly::IOBuf> block;
  for (auto& range : ranges) {
    if (block == nullptr) {
      block = std::move(range);
    } else {
      block->appendToChain(std::move(range));
    }
  }
  co_return block;
}

} // namespace ember::shuffle
