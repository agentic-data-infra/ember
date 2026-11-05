// Map outputs in Kiln (third-party/kiln), the "kiln" shuffle store.
//
// A map task's output is one Kiln chunk, in Kiln's DRAM tier, its local-disk
// tier, or DRAM while it has room and then local disk
// (ember.shuffle.kiln.tier), laid out like a file shuffle data
// file: the frames of reduce partition 0, then partition 1, and so on. Its
// MapStatus names the chunk instead of the executor (kilnLocation()), and its
// partition lengths give each block's byte range, which the driver puts into
// the block URLs. Reduce tasks read their range of the chunk from Kiln
// directly, so the output outlives the executor that wrote it. A chunk Kiln
// has evicted fails the read with a FetchFailure, and the driver recomputes
// the map output as for a lost executor.
//
// With ember.shuffle.spill=false, a writer does not spill to run files:
// a spill appends the buffered pages to its chunk, partition by partition.
// The chunk is then a sequence of such segments, and a block is one byte
// range per segment (MapStatus::segments).
//
// Reduce tasks stream their blocks: a piece of 1 MiB at a time, as Velox
// asks for more, with the next piece read ahead (KilnBlockReader).
//
// Each shuffle's chunks go into a Kiln bucket of its own
// (ShuffleTarget::kilnBucket), which the driver deletes with the shuffle.
// Chunks of outputs stored in Kiln's catalog stay until Kiln evicts them.

#pragma once

#include <optional>

#include <folly/Function.h>
#include <folly/futures/Future.h>

#include "ember/shuffle/FileShuffle.h"
#include "kiln/common/ChunkId.h"

namespace kiln::client {
class BufferPool;
class Client;
}

namespace ember::shuffle {

/// The process's client of the Kiln cluster of metadata server 'metadata'
/// (Kiln's URL list, "tcp://host:port,rdma://host:port", or "host:port"),
/// which all Kiln writers and reads of that cluster share. Kiln picks the
/// transports: RDMA where the URLs, the build and the device allow.
std::shared_ptr<kiln::client::Client> kilnClient(const std::string& metadata);

/// Whether the Kiln clients this process creates from now on reach storage
/// servers on this host through shared memory (Kiln's ClientOptions::useShm,
/// on by default). Off, they use RDMA (or TCP) there too, e.g. to run RDMA
/// on one host. Clients created before keep their setting.
void setKilnUseShm(bool useShm);

/// The size of the buffers Kiln data moves through: the write and staging
/// pools (see kilnReadPool()) hold buffers of this size, and appends and
/// staged reads are cut into pieces of it.
inline constexpr size_t kKilnBufferBytes = 4 << 20;

/// The size of the read pool's buffers, and of the pieces shuffle blocks
/// are read in: small, so that a reader streaming many blocks at once holds
/// little each.
inline constexpr size_t kKilnReadPieceBytes = 1 << 20;

/// Default buffers of each pool: 256 MiB to read and to write, 32 MiB to
/// stage reads.
inline constexpr size_t kDefaultKilnReadBuffers = 256;
inline constexpr size_t kDefaultKilnWriteBuffers = 64;
inline constexpr size_t kDefaultKilnStagingBuffers = 8;

/// The number of buffers in the pools of each Kiln cluster this process
/// uses from now on (pools created before keep theirs).
void setKilnBufferPools(
    size_t readBuffers,
    size_t writeBuffers,
    size_t stagingBuffers = kDefaultKilnStagingBuffers);

/// The bytes of the pools of one Kiln cluster.
int64_t kilnPoolBytes();

/// The process's pools of buffers for moving data from and to the Kiln
/// cluster of metadata server 'metadata', each allocated (with RDMA,
/// registered with the NIC) at its first use, so that no read or append
/// allocates or registers memory:
/// - Shuffle blocks are read piece by piece into the read pool's buffers,
///   which they keep until Velox frees their pages, without a copy. Nobody
///   waits for them: a piece that finds none free is read through the
///   staging pool (see KilnBlockReader).
/// - Reads through the staging pool copy the data out and give the buffers
///   back at once (see co_readKiln()), so its buffers are never held for
///   long, and waiting for them is safe. Kiln catalogs read and write
///   manifests through them too.
/// - Appends hold write buffers while filling and appending, and an output
///   file writer one for its lifetime; they wait for free ones.
std::shared_ptr<kiln::client::BufferPool> kilnReadPool(
    const std::string& metadata);
std::shared_ptr<kiln::client::BufferPool> kilnStagingPool(
    const std::string& metadata);
std::shared_ptr<kiln::client::BufferPool> kilnWritePool(
    const std::string& metadata);

/// The metadata server and chunk (hex) of a kilnLocation().
std::pair<std::string, std::string> parseKilnLocation(
    const std::string& location);

/// A writer for map task 'mapId' of shuffle 'shuffleId' that commits the
/// output to the Kiln cluster of metadata server 'metadata' (as kilnClient()),
/// in Kiln tier 'tier': "dram", "localfs", or "dram+localfs", DRAM if the
/// whole output fits into Kiln's free DRAM, else local FS; and in Kiln bucket
/// 'bucket' (0 for Kiln's default bucket). With 'spill', it spills to
/// 'store''s directory like a file shuffle writer; without, it appends the
/// buffered pages to the chunk instead. It records the committed output in
/// 'store', so that the executor reports it.
std::unique_ptr<ShuffleWriter> createKilnWriter(
    std::shared_ptr<ShuffleBlockStore> store,
    std::string metadata,
    const std::string& tier,
    uint64_t bucket,
    int32_t shuffleId,
    int32_t mapId,
    int32_t numPartitions,
    const std::string& attemptTag,
    bool spill = true);

/// Reads byte ranges ('offset', 'length') of chunk 'id' of the Kiln cluster
/// of metadata server 'metadata' and passes their bytes, in order, to
/// 'sink'. Reads in pieces of kKilnBufferBytes into buffers of the staging
/// pool, as many in parallel as the pool has buffers; each batch goes to
/// 'sink' and back to the pool before the next, so that no read holds
/// buffers while waiting for others.
folly::coro::Task<void> co_readKiln(
    const std::string& metadata,
    kiln::ChunkId id,
    std::vector<std::pair<int64_t, int64_t>> ranges,
    folly::Function<void(const uint8_t*, size_t)> sink);

/// Reads the Kiln block at block URL 'url' (see blockUrl()) piece by piece,
/// each at most kKilnReadPieceBytes, in order. A piece goes into a buffer
/// of the read pool if one is free, and keeps it until the IOBuf is freed;
/// else it is copied through the staging pool. Never waits for a read
/// buffer, as those may be held by pages that wait for this block, e.g. in
/// a merge. Not thread safe.
class KilnBlockReader : public BlockReader {
 public:
  explicit KilnBlockReader(const std::string& url);
  ~KilnBlockReader() override;

  bool atEnd() const override;

  int64_t nextLength() const override;

  /// Does nothing if no read buffer is free.
  void prefetch() override;

  /// Then prefetches the piece after.
  folly::coro::Task<std::unique_ptr<folly::IOBuf>> co_next() override;

  int64_t copiedBytes() const override {
    return copiedBytes_;
  }

  struct State;

 private:
  std::shared_ptr<const State> state_;
  size_t next_{0};
  int64_t copiedBytes_{0};
  std::optional<folly::SemiFuture<std::unique_ptr<folly::IOBuf>>> prefetch_;
};

/// The bytes of the Kiln block at block URL 'url': its pieces from a
/// KilnBlockReader, chained.
folly::coro::Task<std::unique_ptr<folly::IOBuf>> co_fetchKilnBlock(
    std::string url);

} // namespace ember::shuffle
