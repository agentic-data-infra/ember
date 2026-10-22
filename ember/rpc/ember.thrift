// The services between Ember's driver and workers, and between workers for
// shuffle blocks. Compiled by thrift1 into ember/rpc/gen-cpp2 (see BUCK).
//
// Tasks and their results travel as the bytes of TaskDescription and
// TaskResult (ember/worker/TaskDescription.h), the same messages the local
// backend passes in process.

package "ember.dev/rpc"

namespace cpp2 ember.rpc.thrift

include "thrift/annotation/cpp.thrift"

/// Bytes received into, and sent from, an IOBuf without copying.
@cpp.Type{name = "folly::IOBuf"}
typedef binary IOBuf

/// A worker offers its task slots.
struct RegisterWorkerRequest {
  1: string workerId;
  /// "host:port" of the worker's server. The driver connects back there to
  /// launch tasks; other workers fetch shuffle blocks there. Also the
  /// executor's location in shuffle block URLs.
  2: string address;
  3: i32 slots = 1;
}

struct RegisterWorkerResponse {
  1: string appName;
  /// How often the worker sends heartbeats.
  2: i64 heartbeatIntervalMs = 2000;
}

struct HeartbeatRequest {
  1: string workerId;
}

struct HeartbeatResponse {
  /// False if the driver no longer counts the worker as registered, e.g. after
  /// it missed heartbeats. The worker then registers again.
  1: bool registered = true;
}

/// One reduce partition of one map output.
struct BlockId {
  1: i32 shuffleId;
  2: i32 mapId;
  3: i32 reduceId;
}

/// Served by the driver, at its master URL ember://host:port.
service DriverService {
  RegisterWorkerResponse registerWorker(1: RegisterWorkerRequest request);
  HeartbeatResponse heartbeat(1: HeartbeatRequest request);
}

/// Served by every worker: the shuffle blocks its executor wrote.
service ShuffleBlockService {
  IOBuf fetchBlock(1: BlockId block);
}

/// Served by every worker, to the driver it registered with.
service WorkerService extends ShuffleBlockService {
  /// Runs a TaskDescription. The reply is its TaskResult.
  binary launchTask(1: binary task);
  /// Deletes a shuffle's files.
  void removeShuffle(1: i32 shuffleId);
  /// The application is over.
  void shutdown();
}
