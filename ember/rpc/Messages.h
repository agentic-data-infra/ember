// Control messages between the driver and workers. Each travels as a JSON
// payload of its RPC method (Rpc.h). Tasks and their results travel as the
// bytes of TaskDescription and TaskResult.

#pragma once

#include <cstdint>
#include <string>

namespace ember::rpc {

/// kRegisterWorker request: a worker offers its task slots. The driver
/// connects back to 'address' to launch tasks; other workers fetch shuffle
/// blocks from it there.
struct RegisterWorker {
  std::string workerId;
  /// "host:port" of the worker's RPC server. Also the executor's location in
  /// shuffle block URLs.
  std::string address;
  int32_t slots{1};

  std::string serialize() const;
  static RegisterWorker deserialize(const std::string& bytes);
};

/// kRegisterWorker reply.
struct RegisterWorkerReply {
  std::string appName;
  /// How often the worker sends heartbeats.
  int64_t heartbeatIntervalMs{2000};

  std::string serialize() const;
  static RegisterWorkerReply deserialize(const std::string& bytes);
};

/// kHeartbeat request.
struct Heartbeat {
  std::string workerId;

  std::string serialize() const;
  static Heartbeat deserialize(const std::string& bytes);
};

/// kHeartbeat reply.
struct HeartbeatReply {
  /// False if the driver no longer counts the worker as registered, e.g. after
  /// it missed heartbeats. The worker then registers again.
  bool registered{true};

  std::string serialize() const;
  static HeartbeatReply deserialize(const std::string& bytes);
};

/// kRemoveShuffle request.
struct RemoveShuffle {
  int32_t shuffleId{0};

  std::string serialize() const;
  static RemoveShuffle deserialize(const std::string& bytes);
};

/// kFetchBlock request: one reduce partition of one map output held by the
/// worker. The reply is the block's bytes.
struct FetchBlock {
  int32_t shuffleId{0};
  int32_t mapId{0};
  int32_t reduceId{0};

  std::string serialize() const;
  static FetchBlock deserialize(const std::string& bytes);
};

} // namespace ember::rpc
