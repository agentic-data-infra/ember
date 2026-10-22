# ember/worker

| File | Contents |
|------|----------|
| `TaskDescription` | The task message (job, stage, partition, attempt, plan fragment JSON, serialized splits per plan node, config) and the `TaskResult` reply (state, error, user-error flag, `MapStatus`, result pages). Both serialize to bytes. |
| `Executor` | Runs task bytes as a Velox task: deserializes the fragment, adds the splits, waits for completion, and reports the map output or result pages. Owns a `ShuffleBlockStore` under its shuffle directory, registered at its location. |
| `Worker` | A worker process: serves `LaunchTask`, `RemoveShuffle`, `Shutdown` and `FetchBlock` on its port, registers with the driver (retrying until it is up) and sends heartbeats. Each driver gets a fresh `Executor` whose location is the worker's `host:port`. When the driver is done the worker exits, or with `persistent` waits for the next one. |
| `NetworkBlockTransport` | The `BlockTransport` of a worker process: fetches a block from the worker named by its location over RPC, one connection per worker; blocks of this process's own executor are read directly unless told otherwise. A fetch fails after `fetchTimeout` (`--fetch_timeout_ms`, 2 minutes), so a hung worker shows as a fetch failure. `ShuffleBlockHandler` answers `fetchBlock`. |

The `ember_worker` binary (`//bin:ember_worker`) wraps a `Worker`:

```
ember_worker --driver=host:port [--slots=N] [--port=P] [--host=name] \
             [--local_dir=dir] [--persistent]
```
