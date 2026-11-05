// What a running task's shuffle operators get from their executor: its block
// store, its block transport, its IO threads and the slot for fetch failures.
//
// Velox gives an operator or exchange source no way to reach its executor, so
// the executor registers a context under the task's root memory pool while
// the task runs, and readers and writers find it from any pool of the task.

#pragma once

#include <memory>
#include <optional>
#include <string>

#include <folly/Executor.h>
#include <folly/Synchronized.h>

#include "ember/shuffle/ShuffleInterface.h"

namespace facebook::velox::memory {
class MemoryPool;
}

namespace ember::shuffle {

class ShuffleBlockStore;

/// Where a task's block readers leave a FetchFailure for the executor. Velox
/// passes an exchange source's error on to the task as text only, so a reader
/// records the failure here before it fails.
class FetchFailureSlot {
 public:
  /// Records 'failure', unless the task already has one, and throws.
  [[noreturn]] void fail(FetchFailure failure, const std::string& cause);

  /// The first failure recorded.
  std::optional<FetchFailure> failure() const;

 private:
  folly::Synchronized<std::optional<FetchFailure>> failure_;
};

struct ShuffleContext {
  std::shared_ptr<ShuffleBlockStore> store;
  std::shared_ptr<BlockTransport> transport;
  /// Runs a task's work that may wait (BackgroundWork.h).
  std::shared_ptr<folly::Executor> ioExecutor;
  FetchFailureSlot fetchFailures;

  /// The context of the task that owns 'pool'. Throws if there is none.
  static std::shared_ptr<ShuffleContext> of(
      const facebook::velox::memory::MemoryPool& pool);
};

/// Gives the task with query pool 'pool' a ShuffleContext while it runs.
class ShuffleContextScope {
 public:
  ShuffleContextScope(
      const facebook::velox::memory::MemoryPool& pool,
      std::shared_ptr<ShuffleBlockStore> store,
      std::shared_ptr<BlockTransport> transport,
      std::shared_ptr<folly::Executor> ioExecutor);
  ~ShuffleContextScope();

  ShuffleContextScope(const ShuffleContextScope&) = delete;
  ShuffleContextScope& operator=(const ShuffleContextScope&) = delete;

  const ShuffleContext& context() const {
    return *context_;
  }

 private:
  const facebook::velox::memory::MemoryPool& root_;
  const std::shared_ptr<ShuffleContext> context_;
};

} // namespace ember::shuffle
