# ember/api

Ember's Dataframe API. Programs include `ember/Session.h` and
`ember/functions.h`; see `examples/WordCount.cpp`.

| File | Contents |
|------|----------|
| `Session` | Entry point (`Session::builder().master("local[4]").getOrCreate()`), `read()`, `createDataframe`/`inlineTable`. Optimizes plans with Axiom and runs them. |
| `Dataframe` | Immutable value over a chain of plan steps: `select`, `selectExpr`, `filter`, `withColumn`, `groupBy`, `agg`, `orderBy`, `limit`, `distinct`, `unionAll`, `join`; actions `collect`, `count`, `show`; `compile` (cached plans), `schema`, `explain`. |
| `Column`, `functions` | Typed expression builders (`col`, `lit`, `explode`, `split`, `lower`, `count`, ...), `expr("sql")`, and generic `call("fn", ...)` for any registered Velox function. `"x"_c` is `col("x")`. |
| `DataframeReader` | `text(path)` (one `value` column per line) and `parquet(path)`; a directory reads all its data files. |
| `TextFileHandler` | Text format for Axiom's file connector. |
| `FunctionRegistry` | Registers Presto functions (Axiom's base) plus Velox's `sparksql` function package under the `ember_` prefix, and a rewrite hook that resolves scalar calls to those versions, so `split` takes a regex. |
| `Udf.h` | `registerUdf<Fn, Ret, Args...>("name")` over Velox simple functions. |

How a program runs today: transformations only record steps (constant time).
`compile()` builds the Axiom logical plan and optimizes it into a
`MultiFragmentPlan` (`local[N]` gives N tasks per stage), caching both on the
Dataframe; actions compile and run the plan in-process with Axiom's
`LocalRunner`. Errors surface at compile time unless the session sets
`ember.eager_analysis=true`. WordCount plans as three stages:
scan/split/unnest/partial count, then final count and sort after a hash
shuffle on `word`, then a merge to the driver.

Known gaps: aggregates use Presto semantics (the `sparksql` ones are registered
with the `ember_` prefix but not yet mapped); a file is one split; exchanges use Velox's
in-process test exchange until Ember's shuffle replaces them.
