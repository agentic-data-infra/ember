# ember/api

Ember's Dataframe API. Programs include `ember/Session.h` and
`ember/functions.h`; see `examples/WordCount.cpp` and `examples/DomainReport.cpp`.

| File | Contents |
|------|----------|
| `Session` | Entry point (`Session::builder().master("local[4]").getOrCreate()`), `read()`, `createDataframe`/`inlineTable`. Optimizes plans with Axiom and runs them. |
| `Dataframe` | Immutable value over a chain of plan steps: `select`, `selectExpr`, `filter`, `withColumn`, `groupBy`, `agg`, `orderBy`, `limit`, `distinct`, `unionAll`, `join`; actions `collect`, `count`, `show`, `write()`; `cache`/`persist` and `unpersist` (keep a result on the executors, read by later actions); `compile` (cached plans), `schema`, `explain`. |
| `Column`, `functions` | Typed expression builders (`col`, `lit`, `explode`, `split`, `lower`, `count`, ...), `expr("sql")`, and generic `call("fn", ...)` for any registered Velox function. `"x"_c` is `col("x")`. |
| `DataframeReader` | `text(path)` (one `value` column per line), `gensort(path)` (binary `key` and `payload` of 100-byte sort benchmark records) and `parquet(path)`; a directory reads all its data files. A `kiln://<metadata>/data/<dataset>/` path reads a Kiln dataset's files. |
| `DataframeWriter` | `write().gensort(dir)`: each result task writes its rows to `dir/part-NNNNN` as 100-byte records and returns only counts. To a `kiln://<metadata>/data/<dataset>` path, the parts become a new Kiln dataset when the job succeeds. |
| `Udf.h` | `registerUdf<Fn, Ret, Args...>("name")` over Velox simple functions. |

How a program runs today: transformations only record steps (constant time).
`compile()` builds the Axiom logical plan and optimizes it into a
`MultiFragmentPlan` (`local[N]` gives N tasks per stage), caching both on the
Dataframe. Actions hand the plan to the driver's `DagScheduler`
([../driver/README.md](../driver/README.md)), which cuts it into stages and
runs them as tasks on the local backend, with Ember's file shuffle between
stages. Errors surface at compile time unless the session sets
`ember.eager_analysis=true`. `explain()` prints the stages too. WordCount runs
as three stages: scan/split/unnest/partial count, then final count and sort
after a hash shuffle on `word`, then a merge to the driver.

Session config: `ember.local.dir` (shuffle files, default `<tmp>/ember`),
`ember.files.max_split_bytes` (largest byte range of a file per task),
`ember.shuffle.spill_threshold_bytes` (buffered shuffle bytes per map task
before a spill), `ember.reuse.outputs` (keep shuffle outputs for later jobs
with the same stages, default true).

`cache()` returns a Dataframe that plans read like a table: the first action
that needs it computes the original Dataframe and stores the rows on the
executors, one part per task, and later actions read them. The optimizer does
not move work across it, so a sweep of filters over a cached projection
computes the projection once. Cached results live as long as the session (or
until `unpersist()`), and are recomputed if an executor loses them.

Known gaps: aggregates use Presto semantics (the `sparksql` ones are registered
with the `ember_` prefix but not yet mapped); parquet files are one split per
file; stages run one after another; driver and workers share one process
until the RPC step.
