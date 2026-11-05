#include "ember/CachedTable.h"

#include <map>
#include <mutex>

namespace ember {

namespace velox = facebook::velox;

namespace {

std::mutex registryMutex;
std::map<std::string, std::weak_ptr<const CachedTable>> registry;
int64_t tableCounter{0};

} // namespace

CachedTable::~CachedTable() {
  std::lock_guard<std::mutex> l(registryMutex);
  registry.erase(id);
}

std::shared_ptr<const CachedTable> registerCachedTable(
    velox::RowTypePtr schema,
    std::function<facebook::axiom::optimizer::MultiFragmentPlanPtr()> plan) {
  std::lock_guard<std::mutex> l(registryMutex);
  auto table = std::make_shared<const CachedTable>(
      fmt::format("cache_{}", ++tableCounter),
      std::move(schema),
      std::move(plan));
  registry[table->id] = table;
  return table;
}

std::shared_ptr<const CachedTable> findCachedTable(const std::string& id) {
  std::lock_guard<std::mutex> l(registryMutex);
  auto it = registry.find(id);
  return it == registry.end() ? nullptr : it->second.lock();
}

} // namespace ember
