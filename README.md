# ember
Large-scale data processing for agents -- not humans!

Ember is a C++ framework for large-scale data processing, built to be used by
AI agents rather than humans. It particularly targets continuous exploration: AI systems that propose a
hypothesis, run an experiment, evaluate it, refine the hypothesis, and repeat,
thousands of times in parallel, to attack hard problems in science and
engineering. Such agents explore data in a loop of many small, overlapping
queries, so Ember aims for these properties (✅ implemented today, ⬜ planned):

- ✅ Hashed plan DAGs: every stage gets a content key, shown by `explain()`.
- ✅ Reuse of identical stages across jobs in a session.
- ✅ Memoization with `cache()`/`persist()`/`unpersist()`.
- ✅ Fault tolerance through task retry and lineage recompute.
- ⬜ Cost-based materialization: decide what to keep from predicted reuse.
- ⬜ Reuse across sessions and agents, through a persistent driver and catalog.
- ⬜ Shared scans across concurrent queries.
- ⬜ Differential execution: compare a baseline with candidates from where they diverge.
- ⬜ Progressive execution: sampled results with confidence intervals that tighten.
- ⬜ Model and LLM calls as first-class batched operators.
- ⬜ Lineage and guardrails as queryable tables.
- ⬜ Elastic scale-out and scale-in of every component.

Ember offers a Spark-like Dataframe API, uses
[Axiom](https://github.com/facebookincubator/axiom) for query compilation and
[Velox](https://github.com/facebookincubator/velox) for query execution. Dataframe
programs compile into multi-stage Velox plans that run on a driver and a set of
workers.

## Building

Ember builds with [Buck2](https://buck2.build). Velox and Axiom build with CMake
and are wrapped as prebuilt libraries.

Prerequisites: GCC 11+ for Velox and Axiom, GCC 12 (`g++-12`) for Ember
itself, Velox's system dependencies (`velox/scripts/setup-ubuntu.sh`),
ninja, ccache, and buck2 on `PATH`. Ember's RPC uses the fbthrift (and its
`thrift1` compiler) that Velox's setup script installs, built with the same
folly as Velox.

```
# Build Axiom + Velox (uses ~/source/axiom and ~/source/velox by default) and
# generate third-party/generated/deps.bzl.
scripts/build_deps.sh

buck2 build //...
buck2 test //...
```

`AXIOM_SRC`, `VELOX_SRC` and `BUILD_DIR` override the default locations.
Re-run `scripts/build_deps.sh` (or just `scripts/gen_third_party.py`) whenever
Velox or Axiom change.

## Running WordCount

```
buck2 run //examples:word_count -- path/to/text/file/or/directory
```

That runs in one process (`local[*]`). To run it as a driver with separate
worker processes, start the workers pointed at the driver's address, then the
program with master `ember://host:port` and the number of workers to wait
for. Workers keep retrying until the driver is up, so the order does not
matter, and they exit when the program ends.

```
buck2 build //bin:ember_worker //examples:word_count --show-output
# On each worker host (or several times on one):
ember_worker --driver=driver-host:7077 --slots=4
# On the driver host:
word_count path/to/text ember://driver-host:7077 3
```

Input paths must be readable from every worker under the same name. If a
worker dies or hangs mid-job, its tasks, and the shuffle output it held, run
again on the remaining workers.
`scripts/run_local_cluster.sh -n 3 -- word_count path {master} {workers}`
does all of this on one machine.

## More examples

`examples/DomainReport.cpp` reads FineWeb-Edu style parquet (`url`, `score`,
`int_score`, `token_count`) and reports, per web domain, how many documents
and tokens a stricter quality filter (`int_score >= 4`) keeps. It joins two
aggregations over the same input. `examples/check_domain_report.py` checks
its full output against DuckDB:

```
buck2 run //examples:domain_report -- data.parquet 'local[*]' 1 100000000 > report.txt
python3 examples/check_domain_report.py data.parquet report.txt
```

`examples/ThresholdSweep.cpp` sweeps a quality threshold over a per-document
feature computed with a regex over the text. It caches the features with
`Dataframe::cache()`, so only the first threshold computes them:

```
buck2 run //examples:threshold_sweep -- docs/ cache     # or nocache
```

## Python

`python/` binds the same API for Python, with results as pyarrow tables:

```python
from ember import Session
from ember.functions import col, explode, lower, split

ember = Session.builder.appName("WordCount").getOrCreate()
words = ember.read.text(path).select(explode(split(lower(col("value")), r"\W+")).alias("word"))
words.groupBy("word").count().orderBy(col("count").desc()).show()
```

See [python/README.md](python/README.md) for building it.
`examples/word_count.py` and `examples/threshold_sweep.py` are the Python
versions of the examples above, and `examples/check_python_examples.py`
checks that they print the same results.

## Layout

See [ember/README.md](ember/README.md).
