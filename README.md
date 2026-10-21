# ember
Data processing for agents -- not humans!

Ember is a C++ data processing framework designed for AI agents. It offers a
Spark-like DataFrame API, uses [Axiom](https://github.com/facebookincubator/axiom)
for query compilation and [Velox](https://github.com/facebookincubator/velox) for
query execution. DataFrame programs compile into multi-stage Velox plans that run
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

## Layout

See [ember/README.md](ember/README.md).
