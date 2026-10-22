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
ninja, ccache, and buck2 on `PATH`.

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

## Layout

See [ember/README.md](ember/README.md).
