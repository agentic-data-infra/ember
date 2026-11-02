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
// 'pools' also allocates the cluster's buffer pools, if not done yet: a
// driver that only manages buckets never does.
KilnCluster kilnCluster(const std::string& metadata, bool pools = false) {
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
  if (pools && cluster.reads == nullptr) {
    cluster.reads = std::make_shared<kiln::client::BufferPool>(
        *cluster.client, kKilnReadPieceBytes, readBuffers.load());
    cluster.staging = std::make_shared<kiln::client::BufferPool>(
        *cluster.client, kKilnBufferBytes, stagingBuffers.load());
    cluster.writes = std::make_shared<kiln::client::BufferPool>(
        *cluster.client, kKilnBufferBytes, writeBuffers.load());
  }
  return cluster;
}

// Buffers to hold 'bytes'.
size_t buffersFor(int64_t bytes) {
  return (bytes + kKilnBufferBytes - 1) / kKilnBufferBytes;
}

// Takes 'n' buffers from 'pool', waiting until that many are free. More
// than the pool has, for a segment with a page larger than half the pool
// (see createKilnWriter()), are allocated instead, with a warning:
// allocating, and with RDMA registering, them costs time.
std::vector<kiln::Buffer> acquire(
    kiln::client::Client& client,
    kiln::client::BufferPool& pool,
    size_t n) {
  if (n <= pool.count()) {
    return blockingWait(pool.co_acquire(n));
  }
  LOG_FIRST_N(WARNING, 1) << "Needs " << n << " Kiln buffers of "
                          << kKilnBufferBytes << " bytes, the pool has "
                          << pool.count() << ": allocating them";
  std::vector<kiln::Buffer> buffers;
  for (size_t i = 0; i < n; ++i) {
    buffers.push_back(client.allocateBuffer(kKilnBufferBytes));
  }
  return buffers;
}

// Gives back buffers from acquire(); those it allocated go.
void release(
    kiln::client::BufferPool& pool,
    std::vector<kiln::Buffer> buffers) {
  if (buffers.size() <= pool.count()) {
    pool.release(std::move(buffers));
  }
}

// Copies bytes into consecutive buffers, filling each to its capacity.
class BufferFiller {
 public:
  explicit BufferFiller(std::vector<kiln::Buffer>& buffers)
      : buffers_{buffers} {}

  void operator()(const void* data, size_t size) {
    auto* bytes = static_cast<const uint8_t*>(data);
    while (size > 0) {
      auto& buffer = buffers_.at(next_);
      const auto n = std::min(size, buffer.capacity() - buffer.size());
      std::memcpy(buffer.data() + buffer.size(), bytes, n);
      buffer.setSize(buffer.size() + n);
      bytes += n;
      size -= n;
      if (buffer.size() == buffer.capacity()) {
        ++next_;
      }
    }
  }

 private:
  std::vector<kiln::Buffer>& buffers_;
  size_t next_{0};
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
    const auto cluster = kilnCluster(metadata_, /*pools=*/true);
    const auto& client = cluster.client;
    // With the whole size known, an overflow placement puts all of the
    // output into one tier.
    auto chunk = blockingWait(client->co_create(bucket_, placement_, outputBytes()));
    std::vector<int64_t> offsets;
    try {
      // Two buffers: the map task fills one while Kiln appends the other.
      auto buffers = acquire(*client, *cluster.writes, 2);
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
        // The append in flight uses its buffer until it is done. A failed
        // one drops it, and the pool replaces it.
        try {
          awaitAppend();
        } catch (const std::exception&) {
        }
        release(*cluster.writes, std::move(buffers));
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

  // Copies the buffered pages into pooled buffers and starts appending them
  // to the chunk, as one append, without waiting: the map task goes on while
  // Kiln moves the bytes. Kiln takes one append per chunk at a time, so this
  // first waits for the previous one, which gives its buffers back.
  void appendSegment() {
    awaitAppend();
    const auto cluster = kilnCluster(metadata_, /*pools=*/true);
    const auto bytes = outputBytes();
    if (chunk_ == nullptr) {
      // The size of the whole output is not known yet: an overflow placement
      // picks the tier by the first segment.
      chunk_ = blockingWait(cluster.client->co_create(bucket_, placement_, bytes));
    }
    inFlight_ = acquire(*cluster.client, *cluster.writes, buffersFor(bytes));
    const auto offsets = writeBuffered(BufferFiller(inFlight_));
    segments_.push_back(partitionLengths(offsets));
    // co_append uses inFlight_ until it is done; awaitAppend() and
    // discardChunk() wait for that.
    append_ = folly::coro::co_withExecutor(
                  folly::getGlobalCPUExecutor(),
                  chunk_->co_append(std::move(inFlight_)))
                  .start();
  }

  // Waits for the append in flight, if any, and gives its buffers back.
  void awaitAppend() {
    if (append_.has_value()) {
      auto append = std::move(*append_);
      append_.reset();
      SCOPE_EXIT {
        // Moved from on success; as they were on failure.
        release(*kilnCluster(metadata_, true).writes, std::move(inFlight_));
      };
      inFlight_ = std::move(append).get();
    }
  }

  // Drops a chunk that was started but not committed. Its append in flight
  // still uses it, so that one is waited for first.
  void discardChunk() {
    try {
      awaitAppend();
    } catch (const std::exception&) {
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
  // flight and its buffers; and the partition lengths of each append.
  std::unique_ptr<kiln::client::ChunkWriter> chunk_;
  std::optional<folly::SemiFuture<std::vector<kiln::Buffer>>> append_;
  std::vector<kiln::Buffer> inFlight_;
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
  if (!spill) {
    // A segment is appended from write buffers, so that its bytes, up to
    // the threshold plus a page, fit into them. Half the pool leaves room
    // for the page.
    const auto poolBytes = static_cast<int64_t>(
        kilnCluster(metadata, /*pools=*/true).writes->count() *
        kKilnBufferBytes);
    spillThreshold = std::min(spillThreshold, poolBytes / 2);
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
  const auto cluster = kilnCluster(metadata, /*pools=*/true);
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
  kiln::ChunkId id;
  std::vector<std::pair<int64_t, int64_t>> pieces;
};

namespace {

// Reads piece 'i' of 'state''s block into 'buffer', from the read pool,
// and returns it as an IOBuf that gives the buffer back when freed.
folly::coro::Task<std::unique_ptr<folly::IOBuf>> co_readPooled(
    std::shared_ptr<const KilnBlockReader::State> state,
    size_t i,
    kiln::Buffer buffer) {
  const auto& [offset, length] = state->pieces[i];
  auto read = co_await folly::coro::co_awaitTry(co_readPiece(
      *state->cluster.client, state->id, offset, length, buffer));
  if (read.hasException()) {
    state->cluster.reads->release(std::move(buffer));
    co_yield folly::coro::co_error(std::move(read.exception()));
  }
  co_return wrap(state->cluster.reads, std::move(read).value());
}

// Reads piece 'i' of 'state''s block through the staging pool.
folly::coro::Task<std::unique_ptr<folly::IOBuf>> co_readStaged(
    std::shared_ptr<const KilnBlockReader::State> state,
    size_t i) {
  const auto piece = state->pieces[i];
  auto data = folly::IOBuf::create(piece.second);
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

KilnBlockReader::KilnBlockReader(const std::string& url) {
  const auto [metadata, chunk] = parseKilnLocation(parseBlockUrl(url).first);
  auto state = std::make_shared<State>();
  state->metadata = metadata;
  state->cluster = kilnCluster(metadata, /*pools=*/true);
  state->id = kiln::ChunkId::parse(chunk);
  state->pieces = pieces(kilnBlockRanges(url), kKilnReadPieceBytes);
  state_ = std::move(state);
}

KilnBlockReader::~KilnBlockReader() = default;

bool KilnBlockReader::atEnd() const {
  return next_ == state_->pieces.size();
}

int64_t KilnBlockReader::nextLength() const {
  return atEnd() ? 0 : state_->pieces[next_].second;
}

void KilnBlockReader::prefetch() {
  if (atEnd() || prefetch_.has_value()) {
    return;
  }
  auto buffers = state_->cluster.reads->tryAcquire(1);
  if (buffers.empty()) {
    return;
  }
  prefetch_ = folly::coro::co_withExecutor(
                  folly::getGlobalCPUExecutor(),
                  co_readPooled(state_, next_, std::move(buffers[0])))
                  .start();
}

folly::coro::Task<std::unique_ptr<folly::IOBuf>> KilnBlockReader::co_next() {
  VELOX_CHECK(!atEnd());
  std::unique_ptr<folly::IOBuf> piece;
  if (prefetch_.has_value()) {
    auto read = std::move(*prefetch_);
    prefetch_.reset();
    piece = co_await std::move(read);
  } else if (auto buffers = state_->cluster.reads->tryAcquire(1);
             !buffers.empty()) {
    piece = co_await co_readPooled(state_, next_, std::move(buffers[0]));
  } else {
    piece = co_await co_readStaged(state_, next_);
    copiedBytes_ += state_->pieces[next_].second;
  }
  ++next_;
  prefetch();
  co_return piece;
}

folly::coro::Task<std::unique_ptr<folly::IOBuf>> co_fetchKilnBlock(
    std::string url) {
  KilnBlockReader reader{url};
  std::unique_ptr<folly::IOBuf> block;
  while (!reader.atEnd()) {
    auto piece = co_await reader.co_next();
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

void warmUpKiln(const std::string& metadata) {
  kilnCluster(metadata, /*pools=*/true);
}

std::shared_ptr<kiln::client::BufferPool> kilnReadPool(
    const std::string& metadata) {
  return kilnCluster(metadata, /*pools=*/true).reads;
}

std::shared_ptr<kiln::client::BufferPool> kilnStagingPool(
    const std::string& metadata) {
  return kilnCluster(metadata, /*pools=*/true).staging;
}

std::shared_ptr<kiln::client::BufferPool> kilnWritePool(
    const std::string& metadata) {
  return kilnCluster(metadata, /*pools=*/true).writes;
}

} // namespace ember::shuffle
