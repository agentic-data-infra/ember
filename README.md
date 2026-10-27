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
- ✅ Reuse across programs of the same build, through Kiln's catalog (`ember.reuse.kiln`).
- ⬜ Reuse across sessions and agents, through a persistent driver and catalog.
- ⬜ Shared scans across concurrent queries.
- ⬜ Differential execution: compare a baseline with candidates from where they diverge.
- ⬜ Progressive execution: sampled results with confidence intervals that tighten.
- ⬜ Model and LLM calls as first-class batched operators.
- ⬜ Lineage and guardrails as queryable tables.
- ⬜ Elastic scale-out and scale-in of every component.

Ember offers a Dataframe API, uses
[Axiom](https://github.com/facebookincubator/axiom) for query compilation and
[Velox](https://github.com/facebookincubator/velox) for query execution. Dataframe
programs compile into multi-stage Velox plans that run on a driver and a set of
workers.

## Building

Ember builds with [Buck2](https://buck2.build). Velox and Axiom build with CMake
and are wrapped as prebuilt libraries.

Prerequisites: GCC 12 (`gcc-12`, `g++-12`), ninja, ccache, and buck2 on
`PATH`. Velox, Axiom and Ember all build with GCC 12, against Velox's
dependencies built with GCC 12 into `/opt/deps-gcc12`
(`scripts/build_deps_gcc12.sh`, which runs Velox's own setup script; it
leaves `/usr/local` alone). Ember's RPC uses the fbthrift (and its `thrift1`
compiler) from there, built with the same folly as Velox.

```
# Kiln, the storage for the "kiln" shuffle store, is a git submodule.
git submodule update --init

# Build Velox's dependencies into /opt/deps-gcc12 (once; takes a while).
scripts/build_deps_gcc12.sh

# Build Axiom + Velox (uses ~/source/axiom and ~/source/velox by default) and
# generate third-party/generated/deps.bzl.
scripts/build_deps.sh

buck2 build //...
buck2 test //...
```

`PREFIX`, `AXIOM_SRC`, `VELOX_SRC` and `BUILD_DIR` override the default
locations (`BUILD_DIR` defaults to `~/source/axiom/_build/release-gcc12`).
Re-run `scripts/build_deps.sh` (or just `scripts/gen_third_party.py`) whenever
Velox or Axiom change. `scripts/gen_third_party.py` also generates Kiln's
deps file in `third-party/kiln`, which Buck builds as the cell `kiln//`.

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

To keep shuffle outputs in [Kiln](https://github.com/agentic-data-infra/kiln)
instead of on the workers, so that they survive a worker, start a Kiln
cluster and set `ember.shuffle.store=kiln` and
`ember.shuffle.kiln.metadata=<kiln metadata host:port>`; the examples take
extra config from `$EMBER_CONF`:

```
third-party/kiln/scripts/start_local_cluster.sh -d 1 -l 0 -r 4   # prints localhost:7470
EMBER_CONF="ember.shuffle.store=kiln ember.shuffle.kiln.metadata=localhost:7470" \
  buck2 run //examples:word_count -- path/to/text
```

See [ember/shuffle/README.md](ember/shuffle/README.md). Adding
`ember.reuse.kiln=true` also keeps stage outputs in Kiln's catalog, so a
second run of the same program reuses what the first one computed (see
[ember/driver/README.md](ember/driver/README.md)).

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
