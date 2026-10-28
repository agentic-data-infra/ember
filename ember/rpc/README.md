# ember/rpc

RPC between the driver and workers, and between workers for shuffle blocks,
over [fbthrift](https://github.com/facebook/fbthrift) (Rocket transport). The
services are defined in `ember.thrift`; `thrift1` generates their C++ into
`ember/rpc/gen-cpp2/` at build time (`thrift_cpp_library` in
`third-party/thrift.bzl`).

Only this package sees fbthrift. Its public headers expose Ember's own
messages, handler interfaces and clients; the generated code and the Thrift
adapters live in its `.cpp` files, and `:ember_thrift` is a private
dependency, so nothing outside `ember/rpc` can include it. A different
transport would replace these sources without touching the driver or
workers.

| File | Contents |
|------|----------|
| `ember.thrift` | The services and their messages. |
| `Messages` | The messages as plain structs (`RegisterWorkerRequest`, `BlockId`, ...). |
| `Handlers` | `DriverHandler` and `WorkerHandler`, implemented by the driver and workers. |
| `Clients` | `DriverClient`, `WorkerClient`, `ShuffleBlockClient`: `connect(address)`, then coroutine calls (`co_launchTask`, ...) returning `folly::coro::Task`, from any thread. Calls on one client run concurrently; a broken connection or `close()` fails every pending call, and every failure is an `RpcError`. No default call timeout. |
| `RpcServer` | Serves a `DriverHandler` or `WorkerHandler` on a port, with 4 IO and 4 handler threads. Handlers' long calls are coroutines, so long work (a task) runs elsewhere; calls wait for a free handler thread without a queue timeout. `stop()` answers calls in flight; fbthrift aborts the process if one still runs after 10 s. |
| `Rpc` | `RpcError` and `HostAndPort`. |
| `ThriftConnection` | Internal: a Rocket channel on a process-wide IO pool, and the call helper the clients share. |

| Service | Method | From → to | Request → reply |
|---------|--------|-----------|-----------------|
| `DriverService` | `registerWorker` | worker → driver | worker id, address, slots, node → app name, heartbeat interval |
| | `heartbeat` | worker → driver | worker id → whether the driver still knows it |
| `WorkerService` | `launchTask` | driver → worker | `TaskDescription` bytes → `TaskResult` bytes |
| | `removeShuffle` | driver → worker | shuffle id |
| | `shutdown` | driver → worker | |
| | `listFiles` | driver → worker | file format, path → the data files there, with sizes |
| `ShuffleBlockService` | `fetchBlock` | worker → worker | shuffle, map, reduce id → the block's bytes, as an IOBuf |

`WorkerService` extends `ShuffleBlockService`, so a worker serves both on one
port.
