// Runs tasks in this process, as local[N]: one executor with N task slots.
// Tasks still go through their serialized form, and shuffle reads through the
// block transport.

#pragma once

#include "ember/driver/SchedulerBackend.h"

namespace folly {
class CPUThreadPoolExecutor;
}

namespace ember {
class Executor;
}

namespace ember::driver {

struct LocalBackendOptions {
  int32_t numSlots{1};
  /// Threads running Velox drivers.
  int32_t numThreads{4};
  /// Parent of the executor's shuffle directory.
  std::string localDirectory;
};

class LocalBackend : public SchedulerBackend {
 public:
  explicit LocalBackend(LocalBackendOptions options);
  ~LocalBackend() override;

  int32_t totalSlots() const override {
    return options_.numSlots;
  }

  void launchTask(std::string task, ResultCallback onResult) override;

  void removeShuffle(int32_t shuffleId) override;

  void stop() override;

  Executor& executor() {
    return *executor_;
  }

 private:
  const LocalBackendOptions options_;
  std::unique_ptr<Executor> executor_;
  std::unique_ptr<folly::CPUThreadPoolExecutor> slots_;
};

} // namespace ember::driver
