# ember/driver

Turns an optimized plan into stages and runs them.

| File | Contents |
|------|----------|
| `StagePlan` | Cuts an Axiom `MultiFragmentPlan` into stages in dependency order. Rewrites each fragment's plan JSON: a producer's `PartitionedOutput` becomes a `ShuffleWrite`, an `Exchange`/`MergeExchange` becomes a `ShuffleRead`, file scans use serializable handles, and scans of cached tables read the cached output (below). Shuffle ids start as the producing stage's id; the scheduler renumbers them to be unique in its session. |
| `StageKey` | Gives each stage a content key: a Merkle hash over its rewritten plan (node kind, parameters without node and shuffle ids, column names renamed by first use, code version, child keys). Scans add their files' path, size and mtime; a `ShuffleRead` adds the key of the stage it reads, so a key covers all of a stage's upstream. Calls to nondeterministic functions (`rand`, `uuid`, `now`, ...) and non-local inputs leave the node, and everything downstream, without a key. The code version is the driver executable's identity, so a rebuild changes every key. Query config that is not Ember's own (e.g. `session_timezone`), and `ember.files.max_split_bytes`, goes into every key. `explain()` and `onStageCompleted` show the keys. |
| `DagScheduler` | Runs a job: for each stage, enumerates splits (files cut into byte ranges, `ember.files.max_split_bytes`; small files packed into one task), adds shuffle block URLs from the tracker, builds one `TaskDescription` per task and runs the set. Recomputes lost shuffle outputs (below). Collects the result stage's rows. At the end it keeps the outputs of keyed map stages for reuse (below) and removes the rest. `onStageCompleted` sees every stage attempt. |
| `KilnMemo` | With `ember.reuse.kiln`: stored outputs in Kiln's catalog, by stage key, for later programs (below). |
| `TaskScheduler` | Launches a stage's tasks on the backend and waits. Retries a failed task up to `maxTaskAttempts` (4) times; errors from the user's program fail at once. A task that cannot fetch its input is not retried: the task set returns its fetch failure. |
| `MapOutputTracker` | Records each map task's `MapStatus` (location, bytes per partition) and gives reduce task `p` the URLs of its non-empty blocks. Forgets outputs whose executor is lost or could not be fetched from. |
| `SchedulerBackend` | Where tasks run: `launchTask(bytes, callback)`, `totalSlots`, `removeShuffle`, and an executor listener told when executors join or are lost. |
| `LocalBackend` | `local[N]`: one in-process executor with N task slots. Tasks still go through serialized bytes. |
| `ClusterBackend` | `ember://host:port`: serves `DriverService` on the port; workers register there with their address and slots, and the backend sends each task to the worker with the most free slots as a `launchTask` call, whose reply is the task result. A worker whose connection breaks or that misses heartbeats (`ember.cluster.heartbeat_timeout_ms`) is dropped: its running tasks fail and are retried elsewhere, and the listener reports its location lost. |

Source stages run one task per split; other stages run as many tasks as the
shuffle they read has partitions; the result stage runs one task, unless a
sort is range-partitioned (below). Stages run one after another.

## Node-local files

With `ember.files.node_local=true`, each node (machine) has its own files at
a scan's path, as on local disks: the sort benchmark's input, generated on
each node. The driver asks one worker per node for the files there
(`listFiles`), cuts them into ranges as usual, and packs each node's ranges
into tasks of their own that the `ClusterBackend` sends only to that node's
workers. A node that loses all its workers fails its waiting tasks after a
minute. The driver still plans the scan from its own view of the
path, so it must exist there too (e.g. the driver runs on one of the nodes),
and the nodes are those with workers when the job starts. Stages that read
node-local files have no key, since the driver does not know the files.
Only stages with one scan are supported.

## Range-partitioned sort

A global sort without a limit runs as one merge task: the stage before it
sorts each of its partitions and gathers them, and the result task merges
them. With `ember.sort.range_partitions=N`, `StagePlan` makes that stage
write N ranges of the first sort key instead (`RangePartitionFunction` in
`ember/shuffle`), and the result stage runs N merge tasks, one per range. The
driver concatenates their rows in partition order, which is sorted order. Row-
wise nodes (project, filter) may sit above the merge; a limit keeps the single
merge task. The ranges are fixed slices of the key's first 8 bytes, with no
sampling, so they balance only uniformly random keys such as gensort's. Only
VARCHAR and VARBINARY first keys are range-partitioned; other sorts keep the
merge task.

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

## Reusing stage outputs

The scheduler keeps the shuffle output of each map stage that has a key
(StageKey) after its job, in a memo by key, for the life of the session. When
a later job has a stage with the same key and the same number of tasks, that
stage takes the stored shuffle's id instead of a new one and does not run:
its consumers read the stored blocks, and stages only it reads do not run
either. There is no check beyond the key; a key collision is not handled.

Stored shuffles stay registered with the `MapOutputTracker`, so outputs lost
with an executor, or that cannot be fetched, are forgotten as for any
shuffle, and the job reruns just those map tasks (and, recursively, what they
read) before using the rest. `explain()` marks stages `reused` or `not
needed`; `onStageCompleted` reports a reused stage once with `reused` set.
`ember.reuse.outputs=false` turns it off. Stored files go away with the
executors when the session stops.

### Across programs, in Kiln

With map outputs in Kiln (`ember.shuffle.store=kiln`, see
[../shuffle/README.md](../shuffle/README.md)) and `ember.reuse.kiln=true`,
the memo outlives the session. The scheduler also stores each output it
keeps in Kiln's catalog (`kiln/catalog`). Each entry is a memo ref
`memo/ember/<hash of the stage key>` pointing at a manifest chunk that lists
every map output's chunk and partition lengths. A stage whose key the
session's memo lacks is looked up there. If Kiln still holds all its
chunks, the outputs are registered as a stored shuffle, and the stage is
reused as above. A memo with an evicted chunk is dropped by the lookup; the
stage then runs again and its new output is stored. Outputs lost after the
lookup are recomputed as for any stored shuffle. Stage keys include the
driver executable, so a later run of the same program build finds the
memo, and a rebuild does not. Cached tables without a stage key stay in the
session.

## Cached Dataframes

`Dataframe::cache()` registers the Dataframe as a cached table
(`common/CachedTable.h`) and returns a Dataframe that scans it. The optimizer
plans that scan like a file scan, so nothing moves across it: a filter after
`cache()` is not pushed into the cached plan, which is the point of the cut.

`StagePlan` turns the scan into a `ShuffleRead` of the cached output plus a
projection to the scan's columns. Before such a stage runs, the scheduler
materializes the cached table: it plans the cached Dataframe with its last
stage writing one shuffle partition per task (the stage feeding a plain
gather already does; any other last stage gets a one-partition
`ShuffleWrite`) and runs it as a job of its own, which reuses the output
already stored. The output stays in the same memo as above, under the last
stage's key (or the table id if it has none, e.g. it calls `rand`), even with
`ember.reuse.outputs=false`. A reading stage runs one task per stored map
output, and task `p` reads all blocks of map output `p`. Its key covers the
cached output's key. Lost cached outputs are recomputed like any stored
shuffle; `unpersist()` drops the stored output. `explain()` marks a stage
`reads cache_<n> (stored)` or `(not stored yet)`.
