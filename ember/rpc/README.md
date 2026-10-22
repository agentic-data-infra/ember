# ember/rpc

RPC between the driver and workers, and between workers for shuffle blocks,
over [fbthrift](https://github.com/facebook/fbthrift) (Rocket transport). The
services are defined in `ember.thrift`; `thrift1` generates their C++ into
`ember/rpc/gen-cpp2/` at build time (`thrift_cpp_library` in
`third-party/thrift.bzl`).

| File | Contents |
|------|----------|
| `ember.thrift` | The services and their messages. |
| `Rpc` | `RpcError` and `HostAndPort`. |
| `RpcClient` | `RpcClient<Service>::connect(address)`; `call([&](auto& c) { return c.semifuture_x(...); })` returns a future of the reply, from any thread. Calls on one connection run concurrently; a broken connection or `close()` fails every pending call, and every failure is an `RpcError`. No default call timeout. |
| `RpcServer` | Serves a generated `ServiceHandler<Service>` on a port. Handlers return futures, so long work (a task) runs elsewhere. |

| Service | Method | From → to | Request → reply |
|---------|--------|-----------|-----------------|
| `DriverService` | `registerWorker` | worker → driver | worker id, address, slots → app name, heartbeat interval |
| | `heartbeat` | worker → driver | worker id → whether the driver still knows it |
| `WorkerService` | `launchTask` | driver → worker | `TaskDescription` bytes → `TaskResult` bytes |
| | `removeShuffle` | driver → worker | shuffle id |
| | `shutdown` | driver → worker | |
| `ShuffleBlockService` | `fetchBlock` | worker → worker | shuffle, map, reduce id → the block's bytes, as an IOBuf |

`WorkerService` extends `ShuffleBlockService`, so a worker serves both on one
port.
