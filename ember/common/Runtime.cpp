#include "ember/Runtime.h"

#include <unistd.h>

#include <fstream>
#include <mutex>

#include <folly/Conv.h>
#include <folly/Singleton.h>

#include "axiom/connectors/ConnectorMetadataRegistry.h"
#include "axiom/connectors/file/FileConnector.h"
#include "axiom/connectors/file/core/FileConnectorMetadata.h"
#include "ember/CachedConnector.h"
#include "ember/FileSerde.h"
#include "ember/FunctionRegistry.h"
#include "ember/GensortFileHandler.h"
#include "ember/KilnFiles.h"
#include "ember/OutputConnector.h"
#include "ember/ParquetConnector.h"
#include "ember/Stoke.h"
#include "ember/TextFileHandler.h"
#include "ember/shuffle/KilnShuffle.h"
#include "ember/shuffle/Shuffle.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/memory/SharedArbitrator.h"
#include "velox/core/ITypedExpr.h"
#include "velox/core/PlanNode.h"
#include "velox/exec/Exchange.h"
#include "velox/exec/PartitionFunction.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/type/Type.h"

namespace ember {

namespace axiom_connector = facebook::axiom::connector;
namespace velox = facebook::velox;

namespace {

// The machine's memory, or the cgroup's limit if that is lower.
int64_t availableMemoryBytes() {
  auto bytes = int64_t{sysconf(_SC_PHYS_PAGES)} * sysconf(_SC_PAGESIZE);
  for (const auto* path :
       {"/sys/fs/cgroup/memory.max",
        "/sys/fs/cgroup/memory/memory.limit_in_bytes"}) {
    std::string limit;
    if (std::ifstream(path) >> limit) {
      // "max" means no limit.
      if (const auto parsed = folly::tryTo<int64_t>(limit); parsed.hasValue()) {
        bytes = std::min(bytes, *parsed);
      }
    }
  }
  return bytes;
}

} // namespace

void initializeRuntime(int64_t memoryBytes) {
  static std::once_flag once;
  static int64_t budget = 0;
  std::call_once(once, [memoryBytes] {
    const auto available = availableMemoryBytes();
    budget = memoryBytes > 0 ? memoryBytes : available / 5 * 4;
    if (budget > available) {
      LOG(WARNING) << kWorkerMemoryBytes << " is " << budget
                   << ", more than the " << available << " available";
    }
    const auto capacity = budget - shuffle::kilnPoolBytes();
    // What the arbitrator does not hand out is what spilling itself uses.
    const auto arbitratorCapacity = capacity - budget / 10;
    VELOX_USER_CHECK_GT(
        arbitratorCapacity,
        0,
        "{} of {} is too small",
        budget,
        kWorkerMemoryBytes);
    // Ember programs need not call folly::Init; this is the part Velox needs.
    folly::SingletonVault::singleton()->registrationComplete();
    // The shared arbitrator reclaims memory, i.e. makes operators spill, when
    // the tasks together reach its capacity. Ember owns the process memory
    // manager, so initializing it fails if anything else set one first.
    velox::memory::SharedArbitrator::registerFactory();
    velox::memory::MemoryManager::Options options;
    options.arbitratorKind = "SHARED";
    // Velox's own allocator gives freed pages back to the OS, so the
    // process's memory follows what the pools hold; malloc's arenas keep it.
    options.useMmapAllocator = true;
    options.allocatorCapacity = capacity;
    options.arbitratorCapacity = arbitratorCapacity;
    // Velox's defaults are for pools of gigabytes. With these, tasks of any
    // size start small and can be reclaimed from.
    using ArbitratorConfig = velox::memory::SharedArbitrator::ExtraConfig;
    options.extraArbitratorConfigs = {
        {std::string(ArbitratorConfig::kMemoryPoolInitialCapacity), "8MB"},
        {std::string(ArbitratorConfig::kMemoryPoolMinReclaimBytes), "8MB"},
        // A global arbitration round frees half the memory, so that many
        // tasks spill at once rather than one or two per round.
        {std::string(ArbitratorConfig::kGlobalArbitrationMemoryReclaimPct),
         "50"}};
    velox::memory::initializeMemoryManager(options);
    velox::filesystems::registerLocalFileSystem();
    registerKilnFileSystem();
    if (!velox::isRegisteredNamedVectorSerde("Presto")) {
      velox::serializer::presto::PrestoVectorSerde::registerNamedVectorSerde();
    }
    registerFunctions();

    // Plan fragments, expressions and splits travel as JSON.
    velox::Type::registerSerDe();
    velox::core::PlanNode::registerSerDe();
    velox::core::ITypedExpr::registerSerDe();
    velox::exec::registerPartitionFunctionSerDe();
    velox::exec::RemoteConnectorSplit::registerSerDe();
    registerFileSerde();
    shuffle::registerShuffle();

    registerTextFileHandler();
    registerGensortFileHandler();
    auto connector =
        std::make_shared<axiom_connector::file::FileConnector>(kFileConnectorId);
    velox::connector::registerConnector(connector);
    axiom_connector::ConnectorMetadataRegistry::global().insert(
        connector->connectorId(),
        std::make_shared<axiom_connector::file::FileConnectorMetadata>(
            connector.get()));
    registerParquetConnector();
    registerCachedConnector();
    registerOutputConnector();
    registerStokeConnector();
  });
  VELOX_USER_CHECK(
      memoryBytes == 0 || memoryBytes == budget,
      "{} is {} for this process, not {}",
      kWorkerMemoryBytes,
      budget,
      memoryBytes);
}

} // namespace ember
