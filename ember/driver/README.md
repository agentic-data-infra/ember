# ember/driver

Turns an optimized plan into stages and runs them.

| File | Contents |
|------|----------|
| `StagePlan` | Cuts an Axiom `MultiFragmentPlan` into stages in dependency order. Rewrites each fragment's plan JSON: a producer's `PartitionedOutput` becomes a `ShuffleWrite`, an `Exchange`/`MergeExchange` becomes a `ShuffleRead`, and file scans use serializable handles. The shuffle id is the producing stage's id. |
| `DagScheduler` | Runs a job: for each stage, enumerates splits (text files cut into byte ranges, `ember.files.max_split_bytes`), adds shuffle block URLs from the tracker, builds one `TaskDescription` per task and runs the set. Collects the result stage's rows; removes the job's shuffle files at the end. |
| `TaskScheduler` | Launches a stage's tasks on the backend and waits. Retries a failed task up to `maxTaskAttempts` (4) times; errors from the user's program fail at once. |
| `MapOutputTracker` | Records each map task's `MapStatus` (location, bytes per partition) and gives reduce task `p` the URLs of its non-empty blocks. |
| `SchedulerBackend` | Where tasks run: `launchTask(bytes, callback)`, `totalSlots`, `removeShuffle`. |
| `LocalBackend` | `local[N]`: one in-process executor with N task slots. Tasks still go through serialized bytes. |

Source stages run one task per split; other stages run as many tasks as the
shuffle they read has partitions; the result stage runs one task. Stages run
one after another.
