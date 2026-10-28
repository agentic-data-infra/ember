// Executor: runs tasks as Velox tasks. The worker side of Ember.
//
// An executor takes a serialized TaskDescription, rebuilds the plan fragment
// with Velox serde, runs it, and returns a serialized TaskResult. Map tasks
// write their output to the executor's shuffle store; result tasks return
// their rows, or write them to part files and return only counts. Velox's
// spill_enabled lets tasks spill into the shuffle directory; spilling starts
// when a task reaches query_max_memory_per_node. In local mode the driver
// calls an executor in the same process; an ember-worker process wraps one
// behind RPC.

#pragma once

#include <memory>
#include <string>

#include "ember/rpc/Messages.h"
#include "ember/shuffle/FileShuffle.h"

namespace folly {
class CPUThreadPoolExecutor;
}

namespace facebook::velox::memory {
class MemoryPool;
}

namespace ember {

struct ExecutorOptions {
  /// Identifies the executor; readers fetch its shuffle blocks by it.
  std::string location;
  /// Holds shuffle files, and spill files under spill/. Created if missing,
  /// removed with the executor.
  std::string shuffleDirectory;
  /// Threads that run Velox drivers.
  int32_t numThreads{4};
};

class Executor {
 public:
  explicit Executor(ExecutorOptions options);
  ~Executor();

  const std::string& location() const {
    return options_.location;
  }

  /// Runs one task to completion and returns its serialized TaskResult. A
  /// failing task returns a failed result rather than throwing. Thread-safe:
  /// each caller is one task slot.
  std::string runTask(const std::string& taskDescription);

  shuffle::ShuffleBlockStore& blockStore() {
    return *blockStore_;
  }

  /// Deletes the shuffle files of 'shuffleId'.
  void removeShuffle(int32_t shuffleId);

  /// The data files a scan of 'path' in file format 'schema' reads on this
  /// machine, with their sizes: 'path' itself, or the files of a directory
  /// path ending in '/'.
  static std::vector<rpc::FileInfo> listFiles(
      const std::string& schema,
      const std::string& path);

 private:
  const ExecutorOptions options_;
  std::shared_ptr<shuffle::ShuffleBlockStore> blockStore_;
  std::unique_ptr<folly::CPUThreadPoolExecutor> driverExecutor_;
  std::unique_ptr<folly::CPUThreadPoolExecutor> spillExecutor_;
  std::shared_ptr<facebook::velox::memory::MemoryPool> rootPool_;
  std::shared_ptr<facebook::velox::memory::MemoryPool> resultPool_;
  // Holds constants of deserialized plans.
  std::shared_ptr<facebook::velox::memory::MemoryPool> planPool_;
};

} // namespace ember
