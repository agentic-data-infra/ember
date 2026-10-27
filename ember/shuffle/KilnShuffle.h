// Map outputs in Kiln (third-party/kiln), the "kiln" shuffle store.
//
// A map task's output is one Kiln chunk, laid out like a file shuffle data
// file: the frames of reduce partition 0, then partition 1, and so on. Its
// MapStatus names the chunk instead of the executor (kilnLocation()), and its
// partition lengths give each block's byte range, which the driver puts into
// the block URLs. Reduce tasks read their range of the chunk from Kiln
// directly, so the output outlives the executor that wrote it. A chunk Kiln
// has evicted fails the read with a FetchFailure, and the driver recomputes
// the map output as for a lost executor.
//
// The chunks stay in Kiln until it evicts them; nothing removes them when a
// shuffle is removed.

#pragma once

#include "ember/shuffle/FileShuffle.h"

namespace kiln::client {
class Client;
}

namespace ember::shuffle {

/// The process's client of the Kiln cluster of metadata server 'metadata'
/// ("host:port"), which all Kiln writers and reads of that cluster share.
std::shared_ptr<kiln::client::Client> kilnClient(const std::string& metadata);

/// The metadata server and chunk (hex) of a kilnLocation().
std::pair<std::string, std::string> parseKilnLocation(
    const std::string& location);

/// A writer for map task 'mapId' of shuffle 'shuffleId' that commits the
/// output to the Kiln cluster of metadata server 'metadata' ("host:port").
/// It spills to 'store''s directory like a file shuffle writer and records
/// the committed output in 'store', so that the executor reports it.
std::unique_ptr<ShuffleWriter> createKilnWriter(
    std::shared_ptr<ShuffleBlockStore> store,
    std::string metadata,
    int32_t shuffleId,
    int32_t mapId,
    int32_t numPartitions,
    const std::string& attemptTag,
    int64_t spillThreshold);

/// The bytes of the Kiln block at block URL 'url' (see blockUrl()).
folly::coro::Task<std::unique_ptr<folly::IOBuf>> co_fetchKilnBlock(
    std::string url);

} // namespace ember::shuffle
