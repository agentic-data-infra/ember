# ember/api

Ember's Dataframe API. Programs include `ember/Session.h` and
`ember/functions.h`; see `examples/WordCount.cpp`.

| File | Contents |
|------|----------|
| `Session` | Entry point (`Session::builder().master("local[4]").getOrCreate()`), `read()`, `createDataframe`/`inlineTable`. Optimizes plans with Axiom and runs them. |
| `Dataframe` | Immutable value over a chain of plan steps: `select`, `selectExpr`, `filter`, `withColumn`, `groupBy`, `agg`, `orderBy`, `limit`, `distinct`, `unionAll`, `join`; actions `collect`, `count`, `show`; `compile` (cached plans), `schema`, `explain`. |
| `Column`, `functions` | Typed expression builders (`col`, `lit`, `explode`, `split`, `lower`, `count`, ...), `expr("sql")`, and generic `call("fn", ...)` for any registered Velox function. `"x"_c` is `col("x")`. |
| `DataframeReader` | `text(path)` (one `value` column per line) and `parquet(path)`; a directory reads all its data files. |
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
`ember.files.max_split_bytes` (largest text byte range per task),
`ember.shuffle.spill_threshold_bytes` (buffered shuffle bytes per map task
before a spill).

Known gaps: aggregates use Presto semantics (the `sparksql` ones are registered
with the `ember_` prefix but not yet mapped); parquet files are one split per
file; stages run one after another; driver and workers share one process
until the RPC step.
