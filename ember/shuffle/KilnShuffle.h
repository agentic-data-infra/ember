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
// whenever its buffered pages pass the spill threshold, it appends them to
// its chunk, partition by partition, and goes on without waiting for Kiln.
// The chunk is then a sequence of such segments, and a block is one byte
// range per segment (MapStatus::segments), which the reduce task reads at
// once.
//
// Each shuffle's chunks go into a Kiln bucket of its own
// (ember.shuffle.kiln.bucket), which the driver deletes with the shuffle.
// Chunks of outputs stored in Kiln's catalog stay until Kiln evicts them.

#pragma once

#include "ember/shuffle/FileShuffle.h"

namespace kiln::client {
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
    int64_t spillThreshold,
    bool spill = true);

/// The bytes of the Kiln block at block URL 'url' (see blockUrl()): its
/// ranges, read in parallel and chained in order.
folly::coro::Task<std::unique_ptr<folly::IOBuf>> co_fetchKilnBlock(
    std::string url);

} // namespace ember::shuffle
