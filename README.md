# ember
Data processing for agents -- not humans!

Ember is a C++ data processing framework designed for AI agents. It offers a
Spark-like Dataframe API, uses [Axiom](https://github.com/facebookincubator/axiom)
for query compilation and [Velox](https://github.com/facebookincubator/velox) for
query execution. Dataframe programs compile into multi-stage Velox plans that run
on a driver and a set of workers.

## Building

Ember builds with [Buck2](https://buck2.build). Velox and Axiom build with CMake
and are wrapped as prebuilt libraries.

Prerequisites: GCC 11+, Velox's system dependencies (`velox/scripts/setup-ubuntu.sh`),
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

## Layout

See [ember/README.md](ember/README.md).
