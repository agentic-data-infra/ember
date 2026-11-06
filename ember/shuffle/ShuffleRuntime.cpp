#include "ember/shuffle/ShuffleRuntime.h"

#include "velox/common/base/Exceptions.h"

namespace ember::shuffle {

namespace {

folly::Synchronized<std::shared_ptr<ShuffleRuntime>> installed;

} // namespace

ShuffleRuntime::ShuffleRuntime(
    std::shared_ptr<ShuffleBlockStore> store,
    std::shared_ptr<BlockTransport> transport,
    std::shared_ptr<folly::Executor> ioExecutor)
    : store_{std::move(store)},
      transport_{std::move(transport)},
      ioExecutor_{std::move(ioExecutor)} {}

std::shared_ptr<ShuffleRuntime> ShuffleRuntime::current() {
  auto runtime = *installed.rlock();
  VELOX_CHECK_NOT_NULL(runtime, "This process has no executor");
  return runtime;
}

void ShuffleRuntime::failFetch(
    const std::string& url,
    const std::string& cause) {
  failedBlocks_.wlock()->insert(url);
  VELOX_FAIL("Cannot fetch {}: {}", url, cause);
}

std::optional<FetchFailure> ShuffleRuntime::takeFailure(
    const std::vector<std::string>& urls) {
  auto failed = failedBlocks_.wlock();
  std::optional<FetchFailure> first;
  for (const auto& url : urls) {
    if (failed->erase(url) > 0 && !first.has_value()) {
      auto [location, block] = parseBlockUrl(url);
      first = FetchFailure{std::move(location), block};
    }
  }
  return first;
}

ShuffleRuntimeScope::ShuffleRuntimeScope(
    std::shared_ptr<ShuffleBlockStore> store,
    std::shared_ptr<BlockTransport> transport,
    std::shared_ptr<folly::Executor> ioExecutor) {
  auto runtime = std::make_shared<ShuffleRuntime>(
      std::move(store), std::move(transport), std::move(ioExecutor));
  auto current = installed.wlock();
  VELOX_CHECK_NULL(*current, "This process has an executor already");
  *current = std::move(runtime);
}

ShuffleRuntimeScope::~ShuffleRuntimeScope() {
  installed.wlock()->reset();
}

} // namespace ember::shuffle
