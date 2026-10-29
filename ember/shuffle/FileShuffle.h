// File-based sort shuffle: one data file and one index file per map task.
//
// Each map task writes one data file, with the pages of reduce partition 0
// first, then partition 1, and so on, plus an index file of numPartitions + 1
// int64 offsets into the data file. A block (one reduce partition of one map
// output) is the byte range [offset[r], offset[r + 1]) of the data file: a
// sequence of frames, each a uint32 length followed by one serialized page.
//
// The writer buffers pages per partition in memory and spills them to a run
// file (laid out like a data file) when the buffer exceeds a threshold. Commit
// concatenates, per partition, the runs' byte ranges and the buffered pages,
// so pages are never deserialized again.
//
// Files of shuffle S, map task M:
//   <dir>/shuffle_S_M.data, <dir>/shuffle_S_M.index
// The index file is renamed into place last, so its presence marks a
// committed map output. The first attempt to commit wins.

#pragma once

#include <functional>
#include <map>
#include <mutex>

#include "ember/shuffle/ShuffleInterface.h"

namespace ember::shuffle {

/// One executor's shuffle files: creates writers for its map tasks and serves
/// blocks of their committed outputs.
class ShuffleBlockStore : public std::enable_shared_from_this<ShuffleBlockStore> {
 public:
  /// Default per-writer buffer before spilling to a run file.
  static constexpr int64_t kDefaultSpillThreshold = 64 << 20;

  /// 'location' identifies the executor to readers; 'directory' holds the
  /// files and is created if missing.
  ShuffleBlockStore(std::string location, std::string directory);

  const std::string& location() const {
    return location_;
  }

  const std::string& directory() const {
    return directory_;
  }

  /// A writer for map task 'mapId' of shuffle 'shuffleId'. 'attemptTag' makes
  /// temporary file names unique per attempt.
  std::unique_ptr<ShuffleWriter> createWriter(
      int32_t shuffleId,
      int32_t mapId,
      int32_t numPartitions,
      const std::string& attemptTag,
      int64_t spillThreshold = kDefaultSpillThreshold);

  /// Bytes of a committed block.
  std::unique_ptr<folly::IOBuf> readBlock(const BlockId& id) const;

  /// The status of a committed map output, read from its index file.
  MapStatus mapStatus(int32_t shuffleId, int32_t mapId) const;

  bool hasMapOutput(int32_t shuffleId, int32_t mapId) const;

  /// Deletes all files of 'shuffleId'.
  void removeShuffle(int32_t shuffleId);

  std::string dataFile(int32_t shuffleId, int32_t mapId) const;
  std::string indexFile(int32_t shuffleId, int32_t mapId) const;

  /// Stores by location, so a transport in the same process can serve blocks
  /// without going through the network.
  static void registerStore(std::shared_ptr<ShuffleBlockStore> store);
  static void unregisterStore(const std::string& location);
  static std::shared_ptr<ShuffleBlockStore> lookup(const std::string& location);
  /// lookup(), or null if no store is registered at 'location'.
  static std::shared_ptr<ShuffleBlockStore> find(const std::string& location);

  /// Used by writers: renames the temporary files into place unless an
  /// earlier attempt committed. Returns false if one did.
  bool commitFiles(
      int32_t shuffleId,
      int32_t mapId,
      const std::string& tmpData,
      const std::string& tmpIndex);

  /// Used by Kiln writers (KilnShuffle.h): records a map output held in
  /// Kiln unless an earlier attempt committed. Returns false if one did.
  bool commitKilnOutput(int32_t shuffleId, int32_t mapId, MapStatus status);

 private:
  std::vector<int64_t> readOffsets(int32_t shuffleId, int32_t mapId) const;

  const std::string location_;
  const std::string directory_;
  mutable std::mutex commitMutex_;
  // Committed map outputs in Kiln, by (shuffle, map).
  std::map<std::pair<int32_t, int32_t>, MapStatus> kilnOutputs_;
};

/// Base of the shuffle writers: buffers pages per partition in memory, spills
/// them to run files in the store's directory, and at commit hands them to
/// the output grouped by partition.
class PartitionedPageWriter : public ShuffleWriter {
 public:
  /// Receives 'size' bytes at 'data'.
  using Write = std::function<void(const void* data, size_t size)>;

  PartitionedPageWriter(
      std::shared_ptr<ShuffleBlockStore> store,
      int32_t shuffleId,
      int32_t mapId,
      int32_t numPartitions,
      std::string attemptTag,
      int64_t spillThreshold);

  ~PartitionedPageWriter() override;

  void collect(int32_t partition, std::unique_ptr<folly::IOBuf> page) override;

  MapStatus commit() override;

  void abort() override;

  int64_t bytesWritten() const override {
    return bytesWritten_;
  }

  int64_t spillCount() const override {
    return spillCount_;
  }

 protected:
  /// Writes the output with writePartitions() and makes it visible.
  virtual MapStatus commitOutput() = 0;

  /// Writes all pages, partition by partition, through 'write'. Returns the
  /// numPartitions + 1 offsets of the partitions.
  std::vector<int64_t> writePartitions(const Write& write);

  /// The bytes writePartitions() writes.
  int64_t outputBytes() const;

  /// A file name in the store's directory, unique to this attempt.
  std::string tmpPath(const std::string& suffix) const;

  ShuffleBlockStore& store() const {
    return *store_;
  }

  int32_t shuffleId() const {
    return shuffleId_;
  }

  int32_t mapId() const {
    return mapId_;
  }

  bool done() const {
    return done_;
  }

 private:
  // A run of spilled pages, laid out like a data file.
  struct SpillRun {
    std::string path;
    std::vector<int64_t> offsets;
  };

  void spill();
  void removeRuns();

  const std::shared_ptr<ShuffleBlockStore> store_;
  const int32_t shuffleId_;
  const int32_t mapId_;
  const int32_t numPartitions_;
  const std::string attemptTag_;
  const int64_t spillThreshold_;
  std::vector<std::vector<std::unique_ptr<folly::IOBuf>>> pages_;
  int64_t bufferedBytes_{0};
  std::vector<SpillRun> runs_;
  int64_t spillCount_{0};
  int64_t bytesWritten_{0};
  bool done_{false};
};

/// Serves blocks from stores registered in this process.
class LocalBlockTransport : public BlockTransport {
 public:
  folly::coro::Task<std::unique_ptr<folly::IOBuf>> co_fetchBlock(
      std::string location,
      BlockId id) override;
};

/// Splits a block into its pages.
std::vector<std::unique_ptr<folly::IOBuf>> splitFrames(
    std::unique_ptr<folly::IOBuf> block);

} // namespace ember::shuffle
