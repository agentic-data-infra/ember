#include "ember/shuffle/Shuffle.h"

#include <mutex>

#include "ember/shuffle/RangePartitionFunction.h"
#include "ember/shuffle/ShuffleRead.h"
#include "ember/shuffle/ShuffleWrite.h"
#include "velox/common/serialization/Serializable.h"
#include "velox/exec/ExchangeSource.h"

namespace ember::shuffle {

namespace velox = facebook::velox;

void registerShuffle() {
  static std::once_flag once;
  std::call_once(once, [] {
    auto& registry = velox::DeserializationWithContextRegistryForSharedPtr();
    registry.Register("ShuffleWriteNode", ShuffleWriteNode::create);
    registry.Register("ShuffleReadNode", ShuffleReadNode::create);
    registry.Register(
        "RangePartitionFunctionSpec", RangePartitionFunctionSpec::deserialize);
    velox::exec::Operator::registerOperator(
        std::make_unique<ShuffleWriteTranslator>());
    velox::exec::Operator::registerOperator(
        std::make_unique<ShuffleReadTranslator>());
    velox::exec::ExchangeSource::registerFactory(createShuffleBlockSource);
  });
}

} // namespace ember::shuffle
