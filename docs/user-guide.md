# Ember user guide

How to write Ember programs: sessions, reading and writing data,
transformations, reuse, and configuration. The examples are in Python; the
C++ API (`ember/api`) has the same names, and
[Python and C++](#python-and-c) lists where the spellings differ. How to
build Ember and run it, locally or on a cluster, is in the
[README](../README.md).

- [Sessions](#sessions)
- [Reading data](#reading-data)
- [Transformations](#transformations)
- [Aggregations and joins](#aggregations-and-joins)
- [Sorting](#sorting)
- [Getting results](#getting-results)
- [Writing data](#writing-data)
- [Reuse and caching](#reuse-and-caching)
- [Splits and random order](#splits-and-random-order)
- [Shuffle](#shuffle)
- [AI functions](#ai-functions)
- [Statistics](#statistics)
- [Configuration reference](#configuration-reference)
- [Python and C++](#python-and-c)
- [Examples](#examples)

## Sessions

A program starts with a session. Its master says where tasks run:

```python
from ember import Session

ember = (
    Session.builder.appName("Explore")
    .master("local[*]")
    .config("ember.default.parallelism", 16)
    .getOrCreate()
)
```

| Master | Runs |
|---|---|
| `local`, `local[N]`, `local[*]` | In this process: the driver and one executor with 1, N or one task slot per core. Shuffle files go to `ember.local.dir`. |
| `ember://host:port` | This program is the driver and serves at `port`; `ember_worker` processes started with `--driver=host:port` run the tasks. The session waits for `ember.cluster.workers` workers. |

Session config is the one way to configure Ember: every setting is an
`ember.*` key passed with `config(key, value)` on the builder (values may be
strings, numbers or booleans), and it reaches the workers with each task.
The [configuration reference](#configuration-reference) lists all keys. The
C++ examples take extra keys from `$EMBER_CONF`, as space-separated
`key=value` pairs:

```
EMBER_CONF="ember.files.shuffle=global ember.files.shuffle_seed=3" \
  buck2 run //examples:word_count -- path/to/text
```

`getOrCreate()` returns the active session if there is one, and
`Session.active()` returns it. `session.parallelism` is the number of tasks
per stage: what the master implies (for a cluster, the workers' slots at
start) unless `ember.default.parallelism` sets it. `session.stop()` ends the
session; a Python program stops it at exit.

Dataframes are lazy: transformations only build a plan, and actions
(`collect()`, `count()`, `write...`) run it. Errors in a transformation
show up at the first action, or right where they are written with
`ember.eager_analysis=true`.

## Reading data

```python
docs = ember.read.parquet("data/fineweb/")        # a file or a directory
lines = ember.read.text("logs/")                  # one string column "value", one row per line
records = ember.read.gensort("sort-input/")       # binary "key" (10 bytes) and "payload" (90 bytes)
same = ember.read.format("parquet").load("data/fineweb/")
runs = ember.createDataFrame(pyarrow_table)       # a pyarrow Table or RecordBatch
```

A directory reads as one table of all its data files; names that start
with `_` or `.` are skipped. On a cluster, every worker must be able to
read the path under the same name, unless the files are
[node-local](#splits-and-random-order).

Paths of the form `kiln://<metadata>/data/<dataset>/` read a dataset from
[Kiln](https://github.com/agentic-data-infra/kiln)'s catalog (a name
pointing at a manifest of named files; Kiln's README shows how to put and
get them with the `kiln` CLI). Without `<metadata>`,
`kiln:///data/<dataset>/` uses the Kiln cluster of `ember.kiln.metadata`, or
else of `ember.shuffle.kiln.metadata`.

## Transformations

```python
from ember.functions import col, lit, lower, split, explode, length, when

words = (
    lines.select(explode(split(lower("value"), " ")).alias("word"))
    .filter(col("word") != "")
)
scored = (
    docs.withColumn("long", length("text") > 10000)
    .withColumn("bucket", when(col("score") > 3.5, "high").otherwise("low"))
    .selectExpr("url", "bucket", "token_count * 2 AS doubled")
    .drop("long")
)
```

| Method | Does |
|---|---|
| `select(*cols)`, `selectExpr(*sql)` | Projects columns or SQL expressions. |
| `filter(cond)`, `where(cond)` | Keeps rows; `cond` is a Column or a SQL string. |
| `withColumn(name, col)`, `withColumnRenamed(old, new)`, `drop(*names)` | Adds, replaces, renames or removes columns. |
| `distinct()`, `limit(n)` | Removes duplicate rows; keeps the first `n`. |
| `unionAll(other)` (`union`) | Appends the rows of `other`, keeping duplicates. |
| `columns`, `dtypes`, `printSchema()` | The schema. |

`ember.functions` has the column functions: `col`, `lit`, `expr` (a SQL
expression), `when`/`otherwise`, `coalesce`, string functions (`lower`,
`upper`, `length`, `trim`, `ltrim`, `rtrim`, `concat`, `substring`, `split`,
`regexp_replace`, `regexp_extract`), arrays (`explode`, `size`,
`array_contains`), math (`abs`, `round`), and the aggregates below.
`call(name, *cols)` calls any function registered with Velox, which is also
how functions written in C++ are reached. A Column has the arithmetic and
comparison operators, `&`, `|`, `~`, `alias()`, `cast("double")`, `isNull()`,
`isNotNull()`, `isin(...)`, `getField(name)` for struct fields, and
`asc()`/`desc()` (with `_nulls_first`/`_nulls_last`) for sorting.

A `str` argument names a column, except as the other operand of an
operator, where it is a literal: `col("w") != ""`.

## Aggregations and joins

```python
from ember.functions import avg, countAll, countDistinct, sum

by_domain = docs.groupBy("domain").agg(
    countAll().alias("docs"),
    sum("token_count").alias("tokens"),
    avg("score"),
)
totals = docs.agg(countAll(), countDistinct("domain"))
kept = docs.filter(col("int_score") >= 4).groupBy("domain").count()
report = by_domain.join(kept, on="domain", how="left")
```

The aggregates are `count`, `countAll`, `countDistinct`, `sum`, `avg`
(`mean`), `min`, `max`, `first` and `collect_list`; `groupBy(...)` also has
the shortcuts `count()`, `sum(...)`, `avg(...)`, `min(...)` and `max(...)`.

`join(other, on, how="inner")` is an equi-join on one or more columns with
the same names on both sides (`on="id"` or `on=["a", "b"]`); `how` is
`inner`, `left`, `right` or `full` (`outer`).

## Sorting

`orderBy(*cols)` (or `sort`) sorts by columns or by `col(...).desc()` and
friends. Without a limit, a global sort runs as one merge task by default. With
`ember.sort.range_partitions=N` it runs as N tasks instead, each sorting one
range of the first sort key, so that the output parts in name order hold
the sorted rows:

```python
ember = (Session.builder.master("ember://driver:7077")
         .config("ember.sort.range_partitions", 32).getOrCreate())
ember.read.parquet("events/").orderBy("ts").write.parquet("events-sorted/")
```

Range sorts pick their split points from a sample of the first sort key,
which balances any keys but reads the input one extra time.
`ember.sort.sample=false` uses fixed splits of the key's bytes instead,
which only suits uniformly random keys (as gensort's).

## Getting results

| Action | Returns |
|---|---|
| `collect()` | All rows, as a list of tuples. |
| `take(n)`, `head(n)` | The first `n` rows. |
| `count()` | The number of rows. |
| `toArrow()` | A `pyarrow.Table`. |
| `toPandas()` | A pandas DataFrame (needs pandas). |
| `show(n=20, truncate=True)` | Prints the first rows as a table. |
| `explain()` | Prints the logical plan and the stages of the Velox plan, with their keys (see [Reuse and caching](#reuse-and-caching)). |

Results come back through the driver, so large results are better written
out. Actions release the GIL while Ember runs.

## Writing data

```python
rows = report.write.parquet("out/report/")      # snappy Parquet, returns the rows written
records.write.gensort("sorted/")                # gensort records from "key" and "payload"
```

Each task of the stage that makes the rows writes its own part,
`part-NNNNN.parquet` (or `part-NNNNN` for gensort), so the rows never pass
through the driver. The directory may be on a shared file system, on each
worker's local disk, or a Kiln dataset, `kiln://<metadata>/data/<dataset>/`.
Writing fails if the directory holds part files, or the dataset exists,
already.

A write to a Kiln dataset puts each part into one chunk in Kiln's DRAM tier
(`ember.kiln.files.tier=localfs` for the servers' local disks, and
`ember.kiln.files.write_through=localfs` to also keep a copy there). The
driver publishes the dataset when the job succeeds, with all its parts, so
readers see all of it or nothing.

Output connectors take any other target:

```python
df.write.format("my_connector").option("batch", 64).option("mode", "append").save("target")
```

`format(name)` picks the connector, each `option(key, value)` becomes one of
its properties as it is, and `save(target)` writes the rows into the
connector's table `target`, matching columns by name, and returns the rows
written. A session with a Kiln cluster also passes it to the connector, as
the `kiln_metadata` property, unless an option sets that. Connectors are
C++ (Axiom connectors) and are built into Ember.

## Reuse and caching

Ember is made for programs that run many overlapping queries, so it avoids
computing the same thing twice.

**Stage keys.** Every stage of a plan gets a content key: a hash of its
plan, of everything upstream of it, of the files it reads (path, size and
modification time), of the config that can change its result, and of the
program's build. `explain()` shows them. When a later action in the same
session has a stage with the same key, that stage does not run: its
consumers read the output stored by the first one, and `explain()` marks
the stage `reused` (and the stages only it needs `not needed`). This needs no code; `ember.reuse.outputs=false`
turns it off. Outputs lost with a worker are recomputed from their inputs.
Stages that call a nondeterministic function (such as `rand`), or that
scan node-local files, have no key and always run.

**Across programs.** With the shuffle in Kiln (`ember.shuffle.store=kiln`)
and `ember.reuse.kiln=true`, kept outputs also go into Kiln's catalog by
stage key, so a later run of the same program build reuses what an earlier
one computed, for as long as Kiln holds the data. A rebuilt program has new
keys.

**cache().** `cache()` (alias `persist()`) marks a Dataframe's result to be
kept: the first action that needs it stores it on the executors, and later
actions on plans built from the returned Dataframe read it. Use the returned
Dataframe; the original is unchanged. Plans are cut at a cached Dataframe,
so a filter after it is not pushed into it. `unpersist()` drops the stored
result.

```python
from ember.functions import col, length, regexp_extract

features = docs.withColumn("f", length(regexp_extract("text", r"\d+", 0))).cache()
for t in [10, 20, 40]:
    print(t, features.filter(col("f") > t).count())   # only the first computes "f"
```

## Splits and random order

A file scan cuts its files into byte ranges, splits, and hands them to
tasks. A text split reads the lines that start in it, a Parquet split the
row groups that start in it. By default, the scan's bytes spread over
`parallelism` tasks with splits between 4 and 128 MB;
`ember.files.max_split_bytes` sets the split size. A task takes splits
until it holds about that many bytes, each split counting 4 MB extra, so
small files share a task.

With `ember.files.node_local=true`, every node has its own files at the
scan's path (as on each machine's local disks): the driver asks a worker on
each node for its files, and their tasks run only on that node.

Consumers such as training often want rows in random order.
`ember.files.shuffle` hands splits to tasks shuffled:

- `none` (default): files and their splits in order.
- `file`: files one after another (sorted by path), each file's splits
  shuffled.
- `global`: the splits of all files shuffled together.

`ember.files.shuffle_seed` (default 0) seeds the shuffle: the same seed and
files give the same order, so each epoch takes a new seed. Rows inside a
split keep their order, and `file` reorders only files cut into several
splits. A smaller `ember.files.max_split_bytes` gives a finer order but more
and smaller tasks, which costs throughput (4-38% in our dataloader runs
with 4 MiB splits). With node-local files, each node's splits shuffle
separately.

```python
ember = (
    Session.builder.appName("Feed")
    .config("ember.files.shuffle", "global")
    .config("ember.files.shuffle_seed", epoch)
    .getOrCreate()
)
```

## Shuffle

Stages exchange rows through a shuffle: each map task partitions its
output, and each task of the next stage reads one partition from all of
them. Where map outputs go is `ember.shuffle.store`:

- `file` (default): files on the executor that wrote them, under
  `ember.local.dir`. They go away with the executor; a lost output is
  recomputed.
- `kiln`: one Kiln chunk per map output, in the cluster of
  `ember.shuffle.kiln.metadata`, so outputs outlive the workers that wrote
  them. `ember.shuffle.kiln.tier` picks `dram` (default), `localfs`, or
  `dram+localfs` (DRAM if the output fits into Kiln's free DRAM, else local
  disk).

How to build and start a Kiln cluster is in
[Kiln's README](https://github.com/agentic-data-infra/kiln#readme).

Map tasks keep their output in memory and spill to run files only when the
memory manager asks, so an output that fits is written once;
`ember.shuffle.spill_threshold_bytes` also spills above a fixed size. With
the Kiln store, `ember.shuffle.spill=false` appends pages to the Kiln chunk
as the task runs instead. `ember.worker.memory_bytes` is the memory a
process may use (default 80% of the machine or cgroup).

Kiln can move shuffle data and its metadata calls over RDMA. Ember passes
Kiln's URL list through, e.g.
`ember.shuffle.kiln.metadata=tcp://meta:7470,rdma://10.0.0.1:7471`. RDMA is
a build option, off by default: build with `-c kiln.rdma=true` after
linking rlib into the Kiln submodule (`third-party/kiln/scripts/link_rlib.sh`,
see Kiln's README). Without an RDMA device, or in a default build,
everything goes over TCP. More in
[ember/shuffle/README.md](../ember/shuffle/README.md).

## AI functions

`ember.ai.generate(prompt, endpoint=..., output=...)` calls an LLM once per
row (distinct prompts per batch) through an OpenAI-compatible endpoint
defined in the session config, and returns a struct of the answer and a
status:

```python
from ember import ai
from ember.functions import concat, lit

ember = (Session.builder.master("local[*]")
         .config("ember.ai.endpoint.small.url", "http://localhost:8080/v1")
         .config("ember.ai.endpoint.small.model", "qwen2.5-1.5b-instruct")
         .getOrCreate())
judged = ai.generate(concat(lit("Is this educational? "), "text"), endpoint="small",
                     output={"educational": "boolean", "topic": "varchar"})
docs.filter(judged.getField("educational")).groupBy(judged.getField("topic")).count()
```

AI calls take part in reuse like any other expression, and a stage with
failed calls is not kept. Inside `when`/`otherwise`, `&`, `|` and
`coalesce`, a call is made only on the rows that reach it. Put cheap filters
before AI columns, in the same filter as the call, or directly after the
`withColumn` that adds the AI column: then they run first and spare the
model their rows. A filter after a `select`, join or aggregation that
follows the AI column runs after the calls. Endpoints, output schemas, failures and reuse are
in [ai-functions.md](ai-functions.md).

## Statistics

`session.stats()` returns the tasks' stats summed since the session started,
as a dict from name to total, e.g. `aiCalls`, `aiInputTokens` and
`aiOutputTokens`. Take the difference around an action to see what it cost:

```python
before = ember.stats()
judged_docs.write.parquet("judged/")
cost = {k: v - before.get(k, 0) for k, v in ember.stats().items()}
```

A reused stage does no work and adds nothing.

## Configuration reference

| Key | Default | Meaning |
|---|---|---|
| `ember.default.parallelism` | from the master | Tasks per stage. |
| `ember.local.dir` | `$TMPDIR/ember` | Directory for shuffle files. |
| `ember.eager_analysis` | `false` | Check each transformation when it is written. |
| `ember.worker.memory_bytes` | 80% of memory | Bytes this process may use. |
| `ember.cluster.workers` | `1` | Workers a cluster session waits for. |
| `ember.cluster.registration_timeout_ms` | `60000` | How long it waits for them. |
| `ember.cluster.heartbeat_timeout_ms` | `30000` | Missed heartbeats after which a worker is lost. |
| `ember.driver.bind_host` | all interfaces | Interface the driver serves on. |
| `ember.files.max_split_bytes` | bytes / parallelism, 4-128 MB | Largest byte range of a file one task reads. |
| `ember.files.node_local` | `false` | Each node reads its own files at the scan's path. |
| `ember.files.shuffle` | `none` | Split order: `none`, `file` or `global`. |
| `ember.files.shuffle_seed` | `0` | Seed of the split order. |
| `ember.sort.range_partitions` | `0` (one merge task) | Range-sorted tasks of a global sort. |
| `ember.sort.sample` | `true` | Range splits from a sample of the key, else fixed. |
| `ember.reuse.outputs` | `true` | Reuse stages with the same key within a session. |
| `ember.reuse.kiln` | `false` | Also keep and look up outputs in Kiln's catalog. |
| `ember.shuffle.store` | `file` | Map outputs on executors (`file`) or in Kiln (`kiln`). |
| `ember.shuffle.kiln.metadata` | none | Kiln metadata server(s) of the Kiln store. |
| `ember.shuffle.kiln.tier` | `dram` | `dram`, `localfs` or `dram+localfs`. |
| `ember.shuffle.spill` | `true` | Spill to run files (`false`: append to the Kiln chunk). |
| `ember.shuffle.spill_threshold_bytes` | none | Also spill above this many buffered bytes. |
| `ember.kiln.metadata` | `ember.shuffle.kiln.metadata` | Kiln cluster of `kiln://` paths without one. |
| `ember.kiln.files.tier` | `dram` | Kiln tier written dataset parts go to. |
| `ember.kiln.files.write_through` | none | Tier written parts are also written to. |
| `ember.ai.endpoint.<name>.*` | | An AI endpoint: `url`, `model`, `kind`, `api_key_env`, `max_concurrency`, `qps` (see [ai-functions.md](ai-functions.md#endpoints)). |

## Python and C++

The Python module binds the C++ API, so names follow it (`groupBy`,
`orderBy`, `withColumn`, ...). Python spells some things differently:
`alias()` for `as()`; `&`, `|` and `~` for `&&`, `||` and `!`;
`cast("double")` with a type name; and `Session.builder`, `session.read`
and `df.write` are attributes, not calls. Not bound yet: Python UDFs
(register a Velox function in C++ and reach it with `functions.call`) and
stage listeners (`onStageCompleted`).

## Examples

`examples/` has complete programs, in C++ and, for some, in Python:

- `WordCount.cpp`, `word_count.py`: split, explode, group and count.
  `examples/check_python_examples.py` checks that both print the same.
- `DomainReport.cpp`: reads FineWeb-Edu style Parquet (`url`, `score`,
  `int_score`, `token_count`) and reports, per web domain, how many
  documents and tokens a stricter quality filter (`int_score >= 4`) keeps,
  by joining two aggregations over the same input.
  `examples/check_domain_report.py` checks its output against DuckDB:

  ```
  buck2 run //examples:domain_report -- data.parquet 'local[*]' 1 100000000 > report.txt
  python3 examples/check_domain_report.py data.parquet report.txt
  ```

- `ThresholdSweep.cpp`, `threshold_sweep.py`: sweeps a quality threshold
  over a per-document feature computed with a regex, cached with `cache()`
  so that only the first threshold computes it:

  ```
  buck2 run //examples:threshold_sweep -- docs/ cache     # or nocache
  ```

- `SortBenchmark.cpp`: sorts the 100-byte records of
  [sortbenchmark.org](https://sortbenchmark.org)'s gensort by their 10-byte
  key and writes them back, as one merge task or as N range tasks
  (`ember.sort.range_partitions=N`, with `ember.sort.sample=false` since
  gensort's keys are uniform). `scripts/gensort.sh` downloads gensort and
  valsort, writes the input and checks the output:

  ```
  scripts/gensort.sh generate 10000000 /data/sort-input 8     # 1 GB in 8 files
  buck2 run //examples:sort_benchmark -- /data/sort-input /data/sorted
  scripts/gensort.sh validate /data/sorted /data/sort-input
  ```

  It also runs on Kubernetes (see the README's
  [Running on Kubernetes](../README.md#running-on-kubernetes)), with input,
  shuffle and output in Kiln.
