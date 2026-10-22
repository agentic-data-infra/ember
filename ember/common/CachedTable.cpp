#include "ember/CachedTable.h"

#include <map>
#include <mutex>

#include "axiom/connectors/file/FileHandler.h"

namespace ember {

namespace file = facebook::axiom::connector::file;
namespace velox = facebook::velox;

namespace {

std::mutex registryMutex;
std::map<std::string, std::weak_ptr<const CachedTable>> registry;
int64_t tableCounter{0};

class CachedTableHandler : public file::FileHandler {
 public:
  velox::RowTypePtr resolve(
      const std::string& filePath,
      velox::memory::MemoryPool* /*pool*/) const override {
    auto table = findCachedTable(filePath);
    VELOX_USER_CHECK_NOT_NULL(table, "No cached table {}", filePath);
    return table->schema;
  }

 protected:
  std::unique_ptr<velox::connector::DataSource> createDataSource(
      const velox::RowTypePtr& /*outputType*/,
      const velox::RowTypePtr& /*fullSchema*/,
      const velox::connector::ColumnHandleMap& /*columnHandles*/,
      velox::memory::MemoryPool* /*pool*/) const override {
    VELOX_FAIL("Cached tables are read by the driver's scheduler");
  }
};

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

void registerCachedTableHandler() {
  static std::once_flag once;
  std::call_once(once, [] {
    static CachedTableHandler handler;
    file::registerHandler(kCachedSchema, handler);
  });
}

} // namespace ember
