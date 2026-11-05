// Reads shuffles through Velox's own Exchange and MergeExchange. A stage keeps
// the exchanges Axiom planned, and each task's splits are the shuffle block URLs
// of its reduce partition (see ShuffleInterface.h), one per non-empty block.
// This source fetches a block through the BlockTransport. BlockPageReader reads
// a block the same way for a reader outside an exchange.

#pragma once

#include <folly/io/IOBufQueue.h>

#include "ember/shuffle/ShuffleInterface.h"
#include "velox/exec/ExchangeSource.h"

namespace ember::shuffle {

class KilnBlockReader;

/// Velox ExchangeSource factory for shuffle block URLs.
std::shared_ptr<facebook::velox::exec::ExchangeSource> createShuffleBlockSource(
    const std::string& url,
    int destination,
    std::shared_ptr<facebook::velox::exec::ExchangeQueue> queue,
    facebook::velox::memory::MemoryPool* pool);

/// Reads the pages of one stored block, blocking the caller. A block read
/// from Kiln comes in pieces, so that it holds a few read buffers at a time.
class BlockPageReader {
 public:
  explicit BlockPageReader(const std::string& url);
  ~BlockPageReader();

  /// The next pages, in order; none at the end of the block. Throws a
  /// FetchFailure message if the block is gone.
  std::vector<std::unique_ptr<folly::IOBuf>> nextPages();

 private:
  const std::string url_;
  std::string location_;
  BlockId block_;
  std::unique_ptr<KilnBlockReader> kiln_;
  // Bytes read from Kiln but not yet whole pages.
  folly::IOBufQueue pending_{folly::IOBufQueue::cacheChainLength()};
  bool fetched_{false};
};

} // namespace ember::shuffle
