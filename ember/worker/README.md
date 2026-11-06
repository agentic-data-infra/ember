# ember/worker

| File | Contents |
|------|----------|
| `TaskDescription` | The task message (job, stage, partition, attempt, plan fragment JSON, serialized splits per plan node, config) and the `TaskResult` reply (state, error, user-error flag, `MapStatus`, result pages). A result task with `OutputFiles` writes its rows to a part file instead. Both serialize to bytes. |
| `Executor` | Runs task bytes as a Velox task: deserializes the fragment, adds the splits, waits for completion, and reports the map output or result pages. Unless `spill_enabled=false`, a task spills to `<shuffle dir>/spill/` once the tasks together reach the process's capacity (the shared arbitrator reclaims from its operators). Owns a `ShuffleBlockStore` under its shuffle directory, registered at its location. |
| `Worker` | A worker process: serves `LaunchTask`, `RemoveShuffle`, `Shutdown`, `ListFiles` and `FetchBlock` on its port, registers with the driver with its node (`--node`, by default the host name), retrying until it is up, and sends heartbeats. Each driver gets a fresh `Executor` whose location is the worker's `host:port`. When the driver is done the worker exits, or with `persistent` waits for the next one. |
| `NetworkBlockTransport` | The `BlockTransport` of a worker process: fetches a piece of a block from the worker named by its location over RPC, one connection per worker; blocks of this worker's own executor are read directly. A fetch fails after `fetchTimeout` (`--fetch_timeout_ms`, 2 minutes), so a hung worker shows as a fetch failure. `Worker` answers other workers' `fetchBlock` calls from its executor's block store. |

The `ember_worker` binary (`//bin:ember_worker`) wraps a `Worker`:

```
ember_worker --driver=host:port [--slots=N] [--port=P] [--host=name] \
             [--node=name] [--local_dir=dir] [--persistent] \
             [--memory_bytes=B]
```

`--memory_bytes` is what this process may use, by default 80% of the machine or
cgroup, and startup logs a warning if it is more than the machine or cgroup
has. The default is per process, so several workers on one host should get
values that add up. The budget takes off one set of Kiln buffer pools; each
further Kiln cluster the process uses adds its own set on top.
