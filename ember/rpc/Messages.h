// The messages of Ember's RPC, as plain C++ types. They mirror the structs in
// ember.thrift, which only ember/rpc's own sources see. Tasks and their
// results travel as the bytes of TaskDescription and TaskResult.

#pragma once

#include <cstdint>
#include <string>

namespace ember::rpc {

/// A worker offers its task slots to the driver.
struct RegisterWorkerRequest {
  std::string workerId;
  /// "host:port" of the worker's server. The driver connects back there to
  /// launch tasks; other workers fetch shuffle blocks there. Also the
  /// executor's location in shuffle block URLs.
  std::string address;
  int32_t slots{1};
};

struct RegisterWorkerResponse {
  std::string appName;
  /// How often the worker sends heartbeats.
  int64_t heartbeatIntervalMs{2000};
};

/// One reduce partition of one map output.
struct BlockId {
  int32_t shuffleId{0};
  int32_t mapId{0};
  int32_t reduceId{0};
};

} // namespace ember::rpc
