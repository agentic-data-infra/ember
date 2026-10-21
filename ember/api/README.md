# ember/api

Ember's DataFrame API. Programs include `ember/Session.h` and
`ember/functions.h`; see `examples/WordCount.cpp`.

| File | Contents |
|------|----------|
| `Session` | Entry point (`Session::builder().master("local[4]").getOrCreate()`), `read()`, `createDataFrame`/`inlineTable`. Optimizes plans with Axiom and runs them. |
| `DataFrame` | Immutable value over a chain of plan steps: `select`, `selectExpr`, `filter`, `withColumn`, `groupBy`, `agg`, `orderBy`, `limit`, `distinct`, `unionAll`, `join`; actions `collect`, `count`, `show`; `schema`, `explain`, `physicalPlan`. |
| `Column`, `functions` | Typed expression builders (`col`, `lit`, `explode`, `split`, `lower`, `count`, ...), `expr("sql")`, and generic `call("fn", ...)` for any registered Velox function. `"x"_c` is `col("x")`. |
| `DataFrameReader` | `text(path)` (one `value` column per line) and `parquet(path)`; a directory reads all its data files. |
| `TextFileHandler` | Text format for Axiom's file connector. |
| `FunctionRegistry` | Registers Presto functions (Axiom's base) plus Velox's `sparksql` function package under the `ember_` prefix, and a rewrite hook that resolves scalar calls to those versions, so `split` takes a regex. |
| `Udf.h` | `registerUdf<Fn, Ret, Args...>("name")` over Velox simple functions. |

How a program runs today: every DataFrame builds an Axiom logical plan; actions
optimize it into a `MultiFragmentPlan` (`local[N]` gives N tasks per stage) and
run it in-process with Axiom's `LocalRunner`. WordCount plans as three stages:
scan/split/unnest/partial count, then final count and sort after a hash
shuffle on `word`, then a merge to the driver.

Known gaps: aggregates use Presto semantics (the `sparksql` ones are registered
with the `ember_` prefix but not yet mapped); a file is one split; exchanges use Velox's
in-process test exchange until Ember's shuffle replaces them.
