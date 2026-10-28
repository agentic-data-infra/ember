#include "ember/driver/LocalBackend.h"

#include <unistd.h>

#include <atomic>

#include <folly/executors/CPUThreadPoolExecutor.h>

#include "ember/worker/Executor.h"
#include "velox/common/base/Exceptions.h"

namespace ember::driver {

namespace {
std::atomic<int32_t> backendCounter{0};
} // namespace

LocalBackend::LocalBackend(LocalBackendOptions options)
    : options_{std::move(options)} {
  const auto location =
      fmt::format("local-{}-{}", ::getpid(), ++backendCounter);
  executor_ = std::make_unique<Executor>(ExecutorOptions{
      .location = location,
      .shuffleDirectory = fmt::format("{}/{}", options_.localDirectory, location),
      .numThreads = options_.numThreads});
  slots_ = std::make_unique<folly::CPUThreadPoolExecutor>(options_.numSlots);
}

LocalBackend::~LocalBackend() {
  stop();
}

void LocalBackend::launchTask(
    std::string task,
    const std::string& /*node*/,
    ResultCallback onResult) {
  VELOX_CHECK_NOT_NULL(slots_, "Local backend is stopped");
  slots_->add([this, task = std::move(task), onResult = std::move(onResult)]() {
    onResult(executor_->runTask(task));
  });
}

std::vector<rpc::FileInfo> LocalBackend::listFiles(
    const std::string& /*node*/,
    const std::string& schema,
    const std::string& path) {
  return Executor::listFiles(schema, path);
}

void LocalBackend::removeShuffle(int32_t shuffleId) {
  executor_->removeShuffle(shuffleId);
}

void LocalBackend::stop() {
  if (slots_ != nullptr) {
    slots_->join();
    slots_.reset();
    executor_.reset();
  }
}

} // namespace ember::driver
