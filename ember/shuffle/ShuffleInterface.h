// Shuffle interfaces, modelled on Presto C++'s ShuffleInterface.
//
// A map task's ShuffleWrite operator hands serialized pages, tagged with their
// reduce partition, to a ShuffleWriter. The writer commits one data file and
// one index file per map task (see FileShuffle.h). Reduce tasks read their
// partition of each map output as a block, fetched through a BlockTransport
// from the executor that wrote it. In one process the transport reads the
// executor's files directly; across processes it goes over the network, behind
// the same interface.

#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <folly/futures/Future.h>
#include <folly/io/IOBuf.h>

namespace ember::shuffle {

/// One reduce partition of one map task's output.
struct BlockId {
  int32_t shuffleId;
  int32_t mapId;
  int32_t reduceId;

  std::string toString() const;

  bool operator==(const BlockId& other) const = default;
};

/// What a map task reports to the driver when it commits: where its output
/// lives and how many bytes each reduce partition holds.
struct MapStatus {
  /// Executor that holds the output; blocks are fetched from it.
  std::string location;
  std::vector<int64_t> partitionLengths;
};

/// Receives the serialized output of one map task.
class ShuffleWriter {
 public:
  virtual ~ShuffleWriter() = default;

  /// Appends one serialized page to 'partition'.
  virtual void collect(int32_t partition, std::unique_ptr<folly::IOBuf> page) = 0;

  /// Makes the output visible to readers. If another attempt of the same map
  /// task already committed, keeps that output and discards this one.
  virtual MapStatus commit() = 0;

  /// Discards everything written so far.
  virtual void abort() = 0;

  virtual int64_t bytesWritten() const = 0;
  virtual int64_t spillCount() const = 0;
};

/// Fetches shuffle blocks from executors.
class BlockTransport {
 public:
  virtual ~BlockTransport() = default;

  /// Returns the bytes of block 'id' held by the executor at 'location': a
  /// sequence of framed pages (see FileShuffle.h). An empty block has no bytes.
  virtual folly::SemiFuture<std::unique_ptr<folly::IOBuf>> fetchBlock(
      const std::string& location,
      const BlockId& id) = 0;

  /// The process-wide transport used by shuffle reads.
  static void setInstance(std::shared_ptr<BlockTransport> transport);
  static std::shared_ptr<BlockTransport> instance();
};

/// Remote split id of a shuffle block, as read by ShuffleRead:
/// "ember-shuffle://<location>/<shuffleId>/<mapId>/<reduceId>".
std::string blockUrl(const std::string& location, const BlockId& id);

/// True if 'url' names a shuffle block.
bool isBlockUrl(const std::string& url);

/// Parses a blockUrl() into its location and block id.
std::pair<std::string, BlockId> parseBlockUrl(const std::string& url);

/// A reduce task could not fetch a block: its map output is lost, e.g. the
/// executor that holds it died. The driver then recomputes the output rather
/// than retrying the reduce task.
///
/// Velox passes an exchange source's error on to the task as a message only,
/// so the failure travels inside the message (see message() and find()).
struct FetchFailure {
  std::string location;
  BlockId block;

  /// "[fetch-failed <block url>] <cause>".
  std::string message(const std::string& cause) const;

  /// The fetch failure in an error message, if any.
  static std::optional<FetchFailure> find(const std::string& error);
};

} // namespace ember::shuffle
