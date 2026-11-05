#include "ember/shuffle/ShuffleContext.h"

#include <map>

#include "velox/common/base/Exceptions.h"
#include "velox/common/memory/MemoryPool.h"

namespace ember::shuffle {

namespace velox = facebook::velox;

namespace {

// The contexts of running tasks, by root pool.
folly::Synchronized<std::map<
    const velox::memory::MemoryPool*,
    std::shared_ptr<ShuffleContext>>>
    contexts;

} // namespace

void FetchFailureSlot::fail(FetchFailure failure, const std::string& cause) {
  const auto url = blockUrl(failure.location, failure.block);
  failure_.withWLock([&](auto& first) {
    if (!first.has_value()) {
      first = std::move(failure);
    }
  });
  VELOX_FAIL("Cannot fetch {}: {}", url, cause);
}

std::optional<FetchFailure> FetchFailureSlot::failure() const {
  return *failure_.rlock();
}

std::shared_ptr<ShuffleContext> ShuffleContext::of(
    const velox::memory::MemoryPool& pool) {
  auto all = contexts.rlock();
  auto it = all->find(pool.root());
  VELOX_CHECK(it != all->end(), "Pool {} has no running task", pool.name());
  return it->second;
}

ShuffleContextScope::ShuffleContextScope(
    const velox::memory::MemoryPool& pool,
    std::shared_ptr<ShuffleBlockStore> store,
    std::shared_ptr<BlockTransport> transport,
    std::shared_ptr<folly::Executor> ioExecutor)
    : root_{*pool.root()}, context_{std::make_shared<ShuffleContext>()} {
  context_->store = std::move(store);
  context_->transport = std::move(transport);
  context_->ioExecutor = std::move(ioExecutor);
  contexts.wlock()->emplace(&root_, context_);
}

ShuffleContextScope::~ShuffleContextScope() {
  contexts.wlock()->erase(&root_);
}

} // namespace ember::shuffle
