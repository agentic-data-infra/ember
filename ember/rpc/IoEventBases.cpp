#include "ember/rpc/IoEventBases.h"

#include <folly/executors/IOThreadPoolExecutor.h>

namespace ember::rpc {

folly::EventBase* nextIoEventBase() {
  static auto* pool = new folly::IOThreadPoolExecutor(
      4, std::make_shared<folly::NamedThreadFactory>("EmberRpcIO"));
  return pool->getEventBase();
}

} // namespace ember::rpc
