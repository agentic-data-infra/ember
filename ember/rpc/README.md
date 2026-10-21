# ember/rpc

A small RPC over TCP between the driver and workers, and between workers for
shuffle blocks. No IDL and no code generation: a frame is a 16-byte header
(payload length, kind, method, request id) followed by opaque bytes, and one
connection carries any number of concurrent calls, answered in any order.

| File | Contents |
|------|----------|
| `Rpc` | The methods, the frame layout, `RpcError` and `HostAndPort`. |
| `RpcClient` | Connects to a server; `call(method, payload)` returns a future of the reply. A reader thread matches replies to calls; a broken connection fails every pending call. |
| `RpcServer` | Accepts connections, a thread each, and answers requests with the handler set for each method. Handlers return futures, so long work (a task) runs elsewhere. |
| `Messages` | The JSON control messages: `RegisterWorker`, `Heartbeat`, `RemoveShuffle`, `FetchBlock` and their replies. |
| `Socket` | Blocking socket helpers and frame encoding. |

| Method | From → to | Request → reply |
|--------|-----------|-----------------|
| `RegisterWorker` | worker → driver | worker id, address, slots → app name, heartbeat interval |
| `Heartbeat` | worker → driver | worker id → whether the driver still knows it |
| `LaunchTask` | driver → worker | `TaskDescription` bytes → `TaskResult` bytes |
| `RemoveShuffle` | driver → worker | shuffle id → empty |
| `Shutdown` | driver → worker | empty → empty |
| `FetchBlock` | worker → worker | shuffle, map, reduce id → the block's bytes |
