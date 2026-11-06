// What shuffle operators and block readers use from the process's executor:
// its block store, its block transport, its IO threads and the blocks that
// could not be fetched.
//
// A process has one executor at a time, which installs its runtime for as long
// as it runs tasks, so operators and readers find the runtime without knowing
// their task. Velox passes an exchange source's error on to the task as text
// only, so a reader records the block it could not fetch here before it fails,
// and the executor looks up the blocks of a task when the task ends.

#pragma once

#include <memory>
#include <optional>
#include <set>
#include <string>
#include <vector>

#include <folly/Executor.h>
#include <folly/Synchronized.h>

#include "ember/shuffle/ShuffleInterface.h"

namespace ember::shuffle {

class ShuffleBlockStore;

class ShuffleRuntime {
 public:
  ShuffleRuntime(
      std::shared_ptr<ShuffleBlockStore> store,
      std::shared_ptr<BlockTransport> transport,
      std::shared_ptr<folly::Executor> ioExecutor);

  /// The installed runtime. Throws if there is none.
  static std::shared_ptr<ShuffleRuntime> current();

  const std::shared_ptr<ShuffleBlockStore>& store() const {
    return store_;
  }

  const std::shared_ptr<BlockTransport>& transport() const {
    return transport_;
  }

  /// Runs a task's work that may wait (BackgroundWork.h).
  const std::shared_ptr<folly::Executor>& ioExecutor() const {
    return ioExecutor_;
  }

  /// Records that the block at 'url', as the task's split names it, could not
  /// be fetched, and throws.
  [[noreturn]] void failFetch(const std::string& url, const std::string& cause);

  /// Forgets the failures of 'urls', the blocks of a task that ended. Returns
  /// the first one, if any.
  std::optional<FetchFailure> takeFailure(const std::vector<std::string>& urls);

 private:
  const std::shared_ptr<ShuffleBlockStore> store_;
  const std::shared_ptr<BlockTransport> transport_;
  const std::shared_ptr<folly::Executor> ioExecutor_;
  folly::Synchronized<std::set<std::string>> failedBlocks_;
};

/// Installs a ShuffleRuntime while in scope. Throws if one is installed.
class ShuffleRuntimeScope {
 public:
  ShuffleRuntimeScope(
      std::shared_ptr<ShuffleBlockStore> store,
      std::shared_ptr<BlockTransport> transport,
      std::shared_ptr<folly::Executor> ioExecutor);
  ~ShuffleRuntimeScope();

  ShuffleRuntimeScope(const ShuffleRuntimeScope&) = delete;
  ShuffleRuntimeScope& operator=(const ShuffleRuntimeScope&) = delete;
};

} // namespace ember::shuffle
