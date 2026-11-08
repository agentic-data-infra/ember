// Reads shuffles through Velox's own Exchange and MergeExchange. A stage keeps
// the exchanges Axiom planned, and each task's splits are the shuffle block URLs
// of its reduce partition (see ShuffleInterface.h), one per non-empty block.
// This source reads a block as a stream of pieces, as much as Velox asks for,
// each ending where a page does, so that a block holds no read memory between
// requests. BlockPageReader reads a block the same way for a reader outside an
// exchange.

#pragma once

#include "ember/shuffle/ShuffleRuntime.h"
#include "velox/exec/ExchangeSource.h"

namespace ember::shuffle {

/// Velox ExchangeSource factory for shuffle block URLs.
std::shared_ptr<facebook::velox::exec::ExchangeSource> createShuffleBlockSource(
    const std::string& url,
    int destination,
    std::shared_ptr<facebook::velox::exec::ExchangeQueue> queue,
    facebook::velox::memory::MemoryPool* pool);

class BlockPages;

/// Reads the pages of one stored block, blocking the caller, which runs it off
/// the driver thread (BackgroundWork.h). The block comes in pieces, so that it
/// holds little memory at a time; pieces that are copied go into 'pool'.
class BlockPageReader {
 public:
  BlockPageReader(
      const std::string& url,
      std::shared_ptr<facebook::velox::memory::MemoryPool> pool);
  ~BlockPageReader();

  /// The next pages, in order; none at the end of the block. Records a
  /// failed fetch and throws if the block is gone.
  std::vector<std::unique_ptr<folly::IOBuf>> nextPages();

 private:
  const std::unique_ptr<BlockPages> pages_;
};

} // namespace ember::shuffle
