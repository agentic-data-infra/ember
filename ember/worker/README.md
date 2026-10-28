# ember/worker

| File | Contents |
|------|----------|
| `TaskDescription` | The task message (job, stage, partition, attempt, plan fragment JSON, serialized splits per plan node, config) and the `TaskResult` reply (state, error, user-error flag, `MapStatus`, result pages). A result task with `OutputFiles` writes its rows to a part file instead. Both serialize to bytes. |
| `Executor` | Runs task bytes as a Velox task: deserializes the fragment, adds the splits, waits for completion, and reports the map output or result pages. With `spill_enabled`, a task spills to `<shuffle dir>/spill/` once it reaches `query_max_memory_per_node` (the shared arbitrator reclaims from its operators). Owns a `ShuffleBlockStore` under its shuffle directory, registered at its location. |
| `PartFileWriter` | Writes a result task's rows to a hidden temporary file and renames it to `part-NNNNN` on commit, so failed attempts leave no partial part file. |
| `Worker` | A worker process: serves `LaunchTask`, `RemoveShuffle`, `Shutdown`, `ListFiles` and `FetchBlock` on its port, registers with the driver with its node (`--node`, by default the host name), retrying until it is up, and sends heartbeats. Each driver gets a fresh `Executor` whose location is the worker's `host:port`. When the driver is done the worker exits, or with `persistent` waits for the next one. |
| `NetworkBlockTransport` | The `BlockTransport` of a worker process: fetches a block from the worker named by its location over RPC, one connection per worker; blocks of this process's own executor are read directly unless told otherwise. A fetch fails after `fetchTimeout` (`--fetch_timeout_ms`, 2 minutes), so a hung worker shows as a fetch failure. `Worker` answers other workers' `fetchBlock` calls from its executor's block store. |

The `ember_worker` binary (`//bin:ember_worker`) wraps a `Worker`:

```
ember_worker --driver=host:port [--slots=N] [--port=P] [--host=name] \
             [--node=name] [--local_dir=dir] [--persistent]
```
