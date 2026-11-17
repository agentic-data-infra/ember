#include "ember/ai/AiBackend.h"

#include <memory>
#include <mutex>

#include <folly/container/F14Map.h>

#include "ember/ai/HttpBackend.h"

namespace ember::ai {

std::shared_ptr<AiBackend> backendFor(const AiEndpoint& endpoint) {
  static std::mutex mutex;
  // Never destroyed: see backendFor() in the header.
  static auto* backends =
      new folly::F14FastMap<std::string, std::shared_ptr<AiBackend>>();
  std::lock_guard<std::mutex> l(mutex);
  auto& backend = (*backends)[endpoint.key()];
  if (backend == nullptr) {
    backend = makeHttpBackend(endpoint);
  }
  return backend;
}

} // namespace ember::ai
