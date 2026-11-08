#include "ember/shuffle/KilnShuffle.h"

#include <atomic>

#include <folly/ScopeGuard.h>
#include <folly/Uri.h>
#include <folly/coro/BlockingWait.h>
#include <folly/coro/Collect.h>
#include <folly/executors/GlobalExecutor.h>
#include <glog/logging.h>

#include "kiln/client/BufferPool.h"
#include "kiln/client/Client.h"
#include "velox/common/base/Exceptions.h"

namespace ember::shuffle {

namespace velox = facebook::velox;

namespace {

using folly::coro::blockingWait;

// The process's client of a Kiln cluster and its buffer pools, created
// with the first use of each.
struct KilnCluster {
  std::shared_ptr<kiln::client::Client> client;
  std::shared_ptr<kiln::client::BufferPool> reads;
  std::shared_ptr<kiln::client::BufferPool> staging;
  std::shared_ptr<kiln::client::BufferPool> writes;
};

// See setKilnUseShm() and setKilnBufferPools().
std::atomic<bool> useShm{true};
std::atomic<size_t> readBuffers{kDefaultKilnReadBuffers};
std::atomic<size_t> writeBuffers{kDefaultKilnWriteBuffers};
std::atomic<size_t> stagingBuffers{kDefaultKilnStagingBuffers};

// One client per cluster: clients cache where chunks are and keep their
// connections. They are never destroyed: at exit, folly's singletons may be
// gone already, and closing an RDMA metadata client needs its timekeeper.
// Which of a cluster's buffer pools a caller needs.
enum class Pool { kNone, kReads, kStaging, kWrites };

// The cluster's client, and 'pool' allocated if not done yet. Each pool is
// allocated by its first use only: a driver that only reads manifests needs
// just the staging pool, and one that only manages buckets none.
KilnCluster kilnCluster(const std::string& metadata, Pool pool = Pool::kNone) {
  static std::mutex mutex;
  static auto* clusters = new std::map<std::string, KilnCluster>();
  std::lock_guard<std::mutex> l(mutex);
  auto& cluster = (*clusters)[metadata];
  if (cluster.client == nullptr) {
    cluster.client =
        std::make_shared<kiln::client::Client>(kiln::client::ClientOptions{
            .metadata = kiln::MetadataAddress::parse(metadata),
            .useShm = useShm.load()});
  }
  if (pool == Pool::kReads && cluster.reads == nullptr) {
    cluster.reads = std::make_shared<kiln::client::BufferPool>(
        *cluster.client, kKilnReadPieceBytes, readBuffers.load());
  }
  if (pool == Pool::kStaging && cluster.staging == nullptr) {
    cluster.staging = std::make_shared<kiln::client::BufferPool>(
        *cluster.client, kKilnBufferBytes, stagingBuffers.load());
  }
  if (pool == Pool::kWrites && cluster.writes == nullptr) {
    cluster.writes = std::make_shared<kiln::client::BufferPool>(
        *cluster.client, kKilnBufferBytes, writeBuffers.load());
  }
  return cluster;
}

// Streams bytes into a chunk through two pooled buffers: fills one while Kiln
// appends the other, and takes one append per chunk at a time.
class ChunkAppender {
 public:
  ChunkAppender(
      std::shared_ptr<kiln::client::BufferPool> pool,
      kiln::client::ChunkWriter& chunk)
      : pool_{std::move(pool)},
        chunk_{chunk},
        buffers_{blockingWait(pool_->co_acquire(2))} {}

  // The append in flight uses its buffer until it is done. A failed one drops
  // it, and the pool replaces it.
  ~ChunkAppender() {
    try {
      awaitAppend();
    } catch (const std::exception&) {
    }
    pool_->release(std::move(buffers_));
  }

  void write(const void* data, size_t size) {
    auto* bytes = static_cast<const uint8_t*>(data);
    while (size > 0) {
      auto& buffer = buffers_[current_];
      const auto n = std::min(size, buffer.capacity() - buffer.size());
      std::memcpy(buffer.data() + buffer.size(), bytes, n);
      buffer.setSize(buffer.size() + n);
      bytes += n;
      size -= n;
      if (buffer.size() == buffer.capacity()) {
        flush();
      }
    }
  }

  // Waits until all bytes are in the chunk.
  void finish() {
    flush();
    awaitAppend();
  }

 private:
  // Waits for the append in flight, of the other buffer, and empties that
  // buffer for reuse.
  void awaitAppend() {
    if (append_.has_value()) {
      auto future = std::move(*append_);
      append_.reset();
      auto& other = buffers_[current_ ^ 1];
      other = std::move(future).get();
      other.setSize(0);
    }
  }

  // Starts appending the current buffer without waiting and goes on with the
  // other.
  void flush() {
    if (buffers_[current_].size() > 0) {
      awaitAppend();
      append_ = folly::coro::co_withExecutor(
                    folly::getGlobalCPUExecutor(),
                    chunk_.co_append(std::move(buffers_[current_])))
                    .start();
      current_ ^= 1;
    }
  }

  const std::shared_ptr<kiln::client::BufferPool> pool_;
  kiln::client::ChunkWriter& chunk_;
  std::vector<kiln::Buffer> buffers_;
  size_t current_{0};
  std::optional<folly::SemiFuture<kiln::Buffer>> append_;
};

// The data of 'buffer', from 'pool', as an IOBuf that gives the buffer back
// when freed.
std::unique_ptr<folly::IOBuf> wrap(
    std::shared_ptr<kiln::client::BufferPool> pool,
    kiln::Buffer buffer) {
  struct Held {
    std::shared_ptr<kiln::client::BufferPool> pool;
    kiln::Buffer buffer;
  };
  auto* held = new Held{std::move(pool), std::move(buffer)};
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
      bool spill)
      : PartitionedPageWriter(
            std::move(store),
            shuffleId,
            mapId,
            numPartitions,
            std::move(attemptTag)),
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
  void spillPages() override {
    if (spill_) {
      PartitionedPageWriter::spillPages();
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
    const auto cluster = kilnCluster(metadata_, Pool::kWrites);
    const auto& client = cluster.client;
    // With the whole size known, an overflow placement puts all of the
    // output into one tier.
    auto chunk = blockingWait(client->co_create(bucket_, placement_, outputBytes()));
    std::vector<int64_t> offsets;
    try {
      ChunkAppender appender(cluster.writes, *chunk);
      offsets = writePartitions(
          [&](const void* data, size_t size) { appender.write(data, size); });
      appender.finish();
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

  // Appends the buffered pages to the chunk as one segment.
  void appendSegment() {
    const auto cluster = kilnCluster(metadata_, Pool::kWrites);
    if (chunk_ == nullptr) {
      // The size of the whole output is not known yet: an overflow placement
      // picks the tier by the first segment.
      chunk_ = blockingWait(
          cluster.client->co_create(bucket_, placement_, outputBytes()));
    }
    ChunkAppender appender(cluster.writes, *chunk_);
    const auto offsets = writeBuffered(
        [&](const void* data, size_t size) { appender.write(data, size); });
    appender.finish();
    segments_.push_back(partitionLengths(offsets));
  }

  // Drops a chunk that was started but not committed.
  void discardChunk() {
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
  // flight and its buffers; and the partition lengths of each append.
  std::unique_ptr<kiln::client::ChunkWriter> chunk_;
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
      spill);
}

namespace {

// Reads bytes [offset, offset + length) of chunk 'id' into 'buffer' and
// returns it. On failure the buffer stays with the caller.
folly::coro::Task<kiln::Buffer> co_readPiece(
    kiln::client::Client& client,
    kiln::ChunkId id,
    int64_t offset,
    int64_t length,
    kiln::Buffer& buffer) {
  auto read = co_await client.co_read(id, offset, length, std::move(buffer));
  if (read.size() != static_cast<size_t>(length)) {
    buffer = std::move(read);
    VELOX_FAIL("Short read of Kiln chunk {}", id.toString());
  }
  co_return read;
}

// The byte ranges ('offset', 'length') 'ranges', cut into pieces of at
// most 'size' bytes.
std::vector<std::pair<int64_t, int64_t>> pieces(
    const std::vector<std::pair<int64_t, int64_t>>& ranges,
    int64_t size) {
  std::vector<std::pair<int64_t, int64_t>> pieces;
  for (const auto& [offset, length] : ranges) {
    for (int64_t done = 0; done < length; done += size) {
      pieces.emplace_back(offset + done, std::min(length - done, size));
    }
  }
  return pieces;
}

// Reads 'pieces' of chunk 'id' in parallel into 'buffers', one each. On
// failure the buffers of the failed reads stay empty.
folly::coro::Task<void> co_readPieces(
    kiln::client::Client& client,
    kiln::ChunkId id,
    const std::vector<std::pair<int64_t, int64_t>>& pieces,
    size_t first,
    std::vector<kiln::Buffer>& buffers) {
  std::vector<folly::coro::Task<kiln::Buffer>> reads;
  for (size_t i = 0; i < buffers.size(); ++i) {
    const auto& [offset, length] = pieces[first + i];
    reads.push_back(co_readPiece(client, id, offset, length, buffers[i]));
  }
  auto results = co_await folly::coro::collectAllTryRange(std::move(reads));
  for (size_t i = 0; i < results.size(); ++i) {
    if (results[i].hasValue()) {
      buffers[i] = std::move(results[i]).value();
    }
  }
  for (auto& result : results) {
    if (result.hasException()) {
      co_yield folly::coro::co_error(std::move(result.exception()));
    }
  }
}

} // namespace

folly::coro::Task<void> co_readKiln(
    const std::string& metadata,
    kiln::ChunkId id,
    std::vector<std::pair<int64_t, int64_t>> ranges,
    folly::Function<void(const uint8_t*, size_t)> sink) {
  const auto cluster = kilnCluster(metadata, Pool::kStaging);
  auto& pool = *cluster.staging;
  const auto all = pieces(ranges, kKilnBufferBytes);
  for (size_t first = 0; first < all.size();) {
    const auto n = std::min(all.size() - first, pool.count());
    auto buffers = co_await pool.co_acquire(n);
    SCOPE_EXIT {
      pool.release(std::move(buffers));
    };
    co_await co_readPieces(*cluster.client, id, all, first, buffers);
    for (auto& buffer : buffers) {
      sink(buffer.data(), buffer.size());
    }
    first += n;
  }
}

struct KilnBlockReader::State {
  std::string metadata;
  KilnCluster cluster;
  std::shared_ptr<velox::memory::MemoryPool> pool;
  kiln::ChunkId id;
  std::vector<std::pair<int64_t, int64_t>> ranges;
};

namespace {

// 'size' bytes of 'pool' as an empty IOBuf that frees them when freed.
std::unique_ptr<folly::IOBuf> allocate(
    std::shared_ptr<velox::memory::MemoryPool> pool,
    int64_t size) {
  struct Held {
    std::shared_ptr<velox::memory::MemoryPool> pool;
    int64_t size;
  };
  auto* data = pool->allocate(size);
  return folly::IOBuf::takeOwnership(
      data,
      size,
      0,
      [](void* data, void* userData) {
        auto* held = static_cast<Held*>(userData);
        held->pool->free(data, held->size);
        delete held;
      },
      new Held{std::move(pool), size});
}

// Reads 'piece' of 'state''s chunk into 'buffer', from the read pool, and
// returns it as an IOBuf that gives the buffer back when freed.
folly::coro::Task<std::unique_ptr<folly::IOBuf>> co_readPooled(
    std::shared_ptr<const KilnBlockReader::State> state,
    std::pair<int64_t, int64_t> piece,
    kiln::Buffer buffer) {
  auto read = co_await folly::coro::co_awaitTry(co_readPiece(
      *state->cluster.client, state->id, piece.first, piece.second, buffer));
  if (read.hasException()) {
    state->cluster.reads->release(std::move(buffer));
    co_yield folly::coro::co_error(std::move(read.exception()));
  }
  co_return wrap(state->cluster.reads, std::move(read).value());
}

// Reads 'piece' of 'state''s chunk through the staging pool into memory of
// 'state''s Velox pool.
folly::coro::Task<std::unique_ptr<folly::IOBuf>> co_readStaged(
    std::shared_ptr<const KilnBlockReader::State> state,
    std::pair<int64_t, int64_t> piece) {
  auto data = allocate(state->pool, piece.second);
  co_await co_readKiln(
      state->metadata,
      state->id,
      {piece},
      [&](const uint8_t* bytes, size_t size) {
        std::memcpy(data->writableTail(), bytes, size);
        data->append(size);
      });
  co_return data;
}

} // namespace

KilnBlockReader::KilnBlockReader(
    const std::string& url,
    std::shared_ptr<velox::memory::MemoryPool> pool) {
  const auto [metadata, chunk] = parseKilnLocation(parseBlockUrl(url).first);
  auto state = std::make_shared<State>();
  state->metadata = metadata;
  state->cluster = kilnCluster(metadata, Pool::kReads);
  state->pool = std::move(pool);
  state->id = kiln::ChunkId::parse(chunk);
  for (const auto& range : blockUrlRanges(url)) {
    if (range.second > 0) {
      state->ranges.push_back(range);
      remainingBytes_ += range.second;
    }
  }
  state_ = std::move(state);
}

KilnBlockReader::~KilnBlockReader() = default;

bool KilnBlockReader::atEnd() const {
  return remainingBytes_ == 0;
}

int64_t KilnBlockReader::nextLength(int64_t length) const {
  if (prefetch_.has_value()) {
    return prefetchLength_;
  }
  if (range_ == state_->ranges.size()) {
    return 0;
  }
  return std::min(
      {length,
       state_->ranges[range_].second - done_,
       static_cast<int64_t>(kKilnReadPieceBytes)});
}

std::pair<int64_t, int64_t> KilnBlockReader::take(int64_t length) {
  const auto n = nextLength(length);
  VELOX_CHECK_GT(n, 0);
  const auto [offset, size] = state_->ranges[range_];
  const std::pair<int64_t, int64_t> piece{offset + done_, n};
  done_ += n;
  if (done_ == size) {
    ++range_;
    done_ = 0;
  }
  return piece;
}

void KilnBlockReader::prefetch(int64_t length) {
  if (range_ == state_->ranges.size() || prefetch_.has_value()) {
    return;
  }
  auto buffers = state_->cluster.reads->tryAcquire(1);
  if (buffers.empty()) {
    return;
  }
  const auto piece = take(length);
  prefetch_ = folly::coro::co_withExecutor(
                  folly::getGlobalCPUExecutor(),
                  co_readPooled(state_, piece, std::move(buffers[0])))
                  .start();
  prefetchLength_ = piece.second;
}

folly::coro::Task<std::unique_ptr<folly::IOBuf>> KilnBlockReader::co_next(
    int64_t length) {
  VELOX_CHECK(!atEnd());
  std::unique_ptr<folly::IOBuf> piece;
  if (prefetch_.has_value()) {
    auto read = std::move(*prefetch_);
    prefetch_.reset();
    piece = co_await std::move(read);
  } else if (auto buffers = state_->cluster.reads->tryAcquire(1);
             !buffers.empty()) {
    piece = co_await co_readPooled(state_, take(length), std::move(buffers[0]));
  } else {
    piece = co_await co_readStaged(state_, take(length));
    copiedBytes_ += piece->length();
  }
  remainingBytes_ -= piece->computeChainDataLength();
  co_return piece;
}

folly::coro::Task<std::unique_ptr<folly::IOBuf>> co_fetchKilnBlock(
    std::string url,
    std::shared_ptr<velox::memory::MemoryPool> pool) {
  KilnBlockReader reader{url, std::move(pool)};
  std::unique_ptr<folly::IOBuf> block;
  while (!reader.atEnd()) {
    auto piece = co_await reader.co_next(kKilnReadPieceBytes);
    if (block == nullptr) {
      block = std::move(piece);
    } else {
      block->appendToChain(std::move(piece));
    }
  }
  co_return block;
}

void setKilnBufferPools(size_t reads, size_t writes, size_t staging) {
  VELOX_USER_CHECK(
      reads > 0 && writes > 0 && staging > 0, "Kiln buffer pools need buffers");
  readBuffers = reads;
  writeBuffers = writes;
  stagingBuffers = staging;
}

int64_t kilnPoolBytes() {
  return kilnReadPoolBytes() +
      (writeBuffers + stagingBuffers) * kKilnBufferBytes;
}

int64_t kilnReadPoolBytes() {
  return readBuffers * kKilnReadPieceBytes;
}

std::shared_ptr<kiln::client::BufferPool> kilnReadPool(
    const std::string& metadata) {
  return kilnCluster(metadata, Pool::kReads).reads;
}

std::shared_ptr<kiln::client::BufferPool> kilnStagingPool(
    const std::string& metadata) {
  return kilnCluster(metadata, Pool::kStaging).staging;
}

std::shared_ptr<kiln::client::BufferPool> kilnWritePool(
    const std::string& metadata) {
  return kilnCluster(metadata, Pool::kWrites).writes;
}

} // namespace ember::shuffle
