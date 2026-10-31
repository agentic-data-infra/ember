#include "ember/Runtime.h"

#include <mutex>

#include <folly/Singleton.h>

#include "axiom/connectors/ConnectorMetadataRegistry.h"
#include "axiom/connectors/file/FileConnector.h"
#include "axiom/connectors/file/core/FileConnectorMetadata.h"
#include "ember/CachedTable.h"
#include "ember/FileSerde.h"
#include "ember/FunctionRegistry.h"
#include "ember/GensortFileHandler.h"
#include "ember/KilnFiles.h"
#include "ember/ParquetFileHandler.h"
#include "ember/Stoke.h"
#include "ember/TextFileHandler.h"
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

void initializeRuntime() {
  static std::once_flag once;
  std::call_once(once, [] {
    // Ember programs need not call folly::Init; this is the part Velox needs.
    folly::SingletonVault::singleton()->registrationComplete();
    if (!velox::memory::MemoryManager::testInstance()) {
      // The shared arbitrator reclaims memory, i.e. makes operators spill,
      // when a task's pool reaches its capacity.
      velox::memory::SharedArbitrator::registerFactory();
      velox::memory::MemoryManager::Options options;
      options.arbitratorKind = "SHARED";
      velox::memory::initializeMemoryManager(options);
    }
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

    registerParquetFileHandler();
    registerTextFileHandler();
    registerGensortFileHandler();
    registerCachedTableHandler();
    auto connector =
        std::make_shared<axiom_connector::file::FileConnector>(kFileConnectorId);
    velox::connector::registerConnector(connector);
    axiom_connector::ConnectorMetadataRegistry::global().insert(
        connector->connectorId(),
        std::make_shared<axiom_connector::file::FileConnectorMetadata>(
            connector.get()));
    registerStokeConnector();
  });
}

} // namespace ember
