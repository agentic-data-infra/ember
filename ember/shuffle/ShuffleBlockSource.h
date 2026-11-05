// Reads shuffles through Velox's own Exchange and MergeExchange. A stage keeps
// the exchanges Axiom planned, and each task's splits are the shuffle block URLs
// of its reduce partition (see ShuffleInterface.h), one per non-empty block.
// This source fetches a block through the BlockTransport.

#pragma once

#include "velox/exec/ExchangeSource.h"

namespace ember::shuffle {

/// Velox ExchangeSource factory for shuffle block URLs.
std::shared_ptr<facebook::velox::exec::ExchangeSource> createShuffleBlockSource(
    const std::string& url,
    int destination,
    std::shared_ptr<facebook::velox::exec::ExchangeQueue> queue,
    facebook::velox::memory::MemoryPool* pool);

} // namespace ember::shuffle
