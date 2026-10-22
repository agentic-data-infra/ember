# ember/driver

Turns an optimized plan into stages and runs them.

| File | Contents |
|------|----------|
| `StagePlan` | Cuts an Axiom `MultiFragmentPlan` into stages in dependency order. Rewrites each fragment's plan JSON: a producer's `PartitionedOutput` becomes a `ShuffleWrite`, an `Exchange`/`MergeExchange` becomes a `ShuffleRead`, and file scans use serializable handles. The shuffle id is the producing stage's id. |
| `StageKey` | Gives each stage a content key: a Merkle hash over its rewritten plan (node kind, parameters without node and shuffle ids, column names renamed by first use, code version, child keys). Scans add their files' path, size and mtime; a `ShuffleRead` adds the key of the stage it reads, so a key covers all of a stage's upstream. Calls to nondeterministic functions (`rand`, `uuid`, `now`, ...) and non-local inputs leave the node, and everything downstream, without a key. The code version is the driver executable's identity, so a rebuild changes every key. `explain()` and `onStageCompleted` show the keys. |
| `DagScheduler` | Runs a job: for each stage, enumerates splits (text files cut into byte ranges, `ember.files.max_split_bytes`), adds shuffle block URLs from the tracker, builds one `TaskDescription` per task and runs the set. Recomputes lost shuffle outputs (below). Collects the result stage's rows; removes the job's shuffle files at the end. `onStageCompleted` sees every stage attempt. |
| `TaskScheduler` | Launches a stage's tasks on the backend and waits. Retries a failed task up to `maxTaskAttempts` (4) times; errors from the user's program fail at once. A task that cannot fetch its input is not retried: the task set returns its fetch failure. |
| `MapOutputTracker` | Records each map task's `MapStatus` (location, bytes per partition) and gives reduce task `p` the URLs of its non-empty blocks. Forgets outputs whose executor is lost or could not be fetched from. |
| `SchedulerBackend` | Where tasks run: `launchTask(bytes, callback)`, `totalSlots`, `removeShuffle`, and an executor listener told when executors join or are lost. |
| `LocalBackend` | `local[N]`: one in-process executor with N task slots. Tasks still go through serialized bytes. |
| `ClusterBackend` | `ember://host:port`: serves `DriverService` on the port; workers register there with their address and slots, and the backend sends each task to the worker with the most free slots as a `launchTask` call, whose reply is the task result. A worker whose connection breaks or that misses heartbeats (`ember.cluster.heartbeat_timeout_ms`) is dropped: its running tasks fail and are retried elsewhere, and the listener reports its location lost. |

Source stages run one task per split; other stages run as many tasks as the
shuffle they read has partitions; the result stage runs one task. Stages run
one after another.

## Lost shuffle outputs

A map output lives on the worker that wrote it, so it is lost with that
worker. The driver learns of it in two ways:

- The backend drops the worker (broken connection, missed heartbeats) and
  reports its location lost. The tracker forgets every output there and
  ignores outputs reported from it until a worker registers there again.
- A reduce task fails to fetch a block (the worker is gone, hangs past the
  workers' fetch timeout, or lost the file). The shuffle read tags the error
  with the block, the task result carries it as a fetch failure, and the
  tracker forgets that shuffle's outputs at the block's location.

Before a stage runs, the scheduler reruns, on the remaining workers, the map
tasks of each input whose outputs are missing, recursively for their own
inputs. Then the stage runs a new attempt for only the partitions it still
lacks. A stage that ends `maxStageAttempts` (4) attempts with partitions
missing fails the job.
