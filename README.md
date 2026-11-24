# ember
Large-scale data processing for agents!

## Overview

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
- ✅ RDMA-based shuffle: map outputs in Kiln outlive their workers and move over one-sided RDMA (`ember.shuffle.store=kiln`, see [Shuffle](docs/user-guide.md#shuffle)).
- ✅ Input and output datasets in Kiln: `kiln://<metadata>/data/<dataset>/` paths (see [Reading data](docs/user-guide.md#reading-data)).
- ⬜ Cost-based materialization: decide what to keep from predicted reuse.
- ✅ Reuse across programs of the same build, through Kiln's catalog (`ember.reuse.kiln`).
- ⬜ Reuse across sessions and agents, through a persistent driver and catalog.
- ⬜ Shared scans across concurrent queries.
- ⬜ Differential execution: compare a baseline with candidates from where they diverge.
- ⬜ Progressive execution: sampled results with confidence intervals that tighten.
- ✅ LLM calls in Dataframe expressions: `ai.generate` with typed outputs, made only on the rows that reach them, asynchronous on Velox's RPC operator, and reused like other stages (see [AI functions](docs/user-guide.md#ai-functions)).
- ⬜ Models next to the data: node-local or in-process inference on the job's GPUs.
- ⬜ Lineage and guardrails as queryable tables.
- ⬜ Elastic scale-out and scale-in of every component.

Ember offers a Dataframe API, uses
[Axiom](https://github.com/facebookincubator/axiom) for query optimization and
distributed planning, and [Velox](https://github.com/facebookincubator/velox)
for query execution. Dataframe
programs compile into multi-stage Velox plans that run on a driver and a set of
workers.

- [docs/user-guide.md](docs/user-guide.md): writing Ember programs: reading
  and writing data, transformations, reuse and caching, and every config
  key.
- [docs/comparison.md](docs/comparison.md): Ember and other data processing
  systems.

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

# Build Axiom + Velox (uses ~/source/axiom and ~/source/velox by default, which
# must be at the versions pinned in the script) and generate
# third-party/generated/deps.bzl.
scripts/build_deps.sh

buck2 build //...
buck2 test //...
```

`PREFIX`, `AXIOM_SRC`, `VELOX_SRC` and `BUILD_DIR` override the default
locations (`BUILD_DIR` defaults to `~/source/axiom/_build/release-gcc12`).
Re-run `scripts/build_deps.sh` (or just `scripts/gen_third_party.py`) whenever
Velox or Axiom change. `scripts/gen_third_party.py` also generates Kiln's
deps file in `third-party/kiln`, which Buck builds as the cell `kiln//`. Buck
does not see that the Axiom and Velox libraries changed: run `buck2 clean` after
rebuilding them.

### Updating Axiom and Velox

Ember builds with [our fork of Axiom](https://github.com/agentic-data-infra/axiom):
upstream Axiom plus a few small commits, and Velox at the commit Axiom's `velox`
submodule pins. The fork is rebased onto upstream every couple of weeks:

- Each rebase is a new branch `ember-YYYYMMDD` (the date it was cut from
  upstream `main`). Branches are never force-pushed.
- Each version Ember builds with is an annotated tag `ember-YYYYMMDD-N` on that
  branch, `-1` first and a higher number for later commits such as fixes.
- `AXIOM_REF` at the top of `scripts/build_deps.sh` is always a tag. Old
  branches and tags stay, so that older Ember commits still build.

`scripts/bump_axiom.sh` does a rebase in separate trees, by default
`~/source/axiom-next` and `~/source/velox-next` (`AXIOM_SRC`, `VELOX_SRC`), from
an Ember checkout on a new branch:

```
scripts/bump_axiom.sh
```

It creates today's branch from upstream `main`, cherry-picks the fork commits
of the previous `ember-YYYYMMDD` branch, moves Velox to Axiom's pin, runs
`scripts/build_deps.sh`, and runs the Axiom and Ember tests. On a conflict or
a failure it stops; fix it (in Axiom or in Ember) and run it again. It does not
push or tag: at the end it prints the commands that do and that set
`AXIOM_REF` to the tag.

## Running locally

By default (master `local[*]`, or `local[N]`), the driver and one executor
run inside the program's own process: no `ember_worker`, Kiln or cluster is
needed, and shuffle files go to `ember.local.dir` (default `$TMPDIR/ember`,
so `/tmp/ember`). WordCount in C++:

```
buck2 run //examples:word_count -- path/to/text/file/or/directory
```

### Python

`python/` binds the same API for Python, with results as pyarrow tables. It
ships as a wheel for Linux x86-64 (glibc 2.35 or newer) and CPython 3.10.
`scripts/build_wheel.sh` builds the module and the wheel, with the shared
libraries it links bundled in, into `dist/`:

```
scripts/build_wheel.sh                                  # dist/ember-0.1.0-cp310-cp310-manylinux_2_35_x86_64.whl
python3 -m pip install --user dist/ember-*.whl          # pulls in pyarrow
python3 -m pip show ember                               # is it installed, and where?
```

Any Python 3.10 with pip works; for the repository's `.venv`, which has no
pip, use `uv pip install -p .venv/bin/python dist/ember-*.whl`. The version
stays 0.1.0, so after a rebuild add `--force-reinstall`. For development,
`pip install -e python/` uses the module built in place instead (see
[python/README.md](python/README.md)).

Small queries are a few lines:

```python
import pyarrow as pa
from ember import Session
from ember.functions import avg, col, countAll

ember = Session.builder.appName("Local").master("local[4]").getOrCreate()
runs = ember.createDataFrame(pa.table({"model": ["a", "a", "b"], "loss": [0.9, 0.7, 0.6]}))
runs.filter(col("loss") < 0.8).groupBy("model").agg(countAll().alias("runs"), avg("loss")).show()
ember.read.parquet("data.parquet").selectExpr("model", "loss * 2 AS doubled").collect()  # list of tuples
```

`toArrow()` returns a pyarrow Table and `explain()` prints the plan; the
[user guide](docs/user-guide.md) covers the rest of the API.
`examples/word_count.py` and `examples/threshold_sweep.py` are the Python
versions of the C++ examples:

```
GLOG_minloglevel=2 python3 examples/word_count.py path/to/text   # GLOG_minloglevel=2 hides the info logs
```

With an editable install, `python3 -c` or the interpreter started in a
directory that holds an `ember` directory, such as the repository root or
`/tmp` (the local directory), imports that directory instead of the
package. Scripts and wheel installs are not affected.

## Running on a cluster

On a cluster, the program is the driver and `ember_worker` processes run its
tasks: the program connects with master `ember://host:port` and waits for
`ember.cluster.workers` workers.

### Running manually

Start the workers pointed at the driver's address, then the program with
its master and the number of workers. Workers keep retrying until the driver is up, so the order does not
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
does all of this on one machine. A Python program connects the same way,
with `.master("ember://driver-host:7077")` and
`.config("ember.cluster.workers", "3")` on `Session.builder`;
`examples/word_count.py` takes the same arguments as `word_count`.

#### Shuffle in Kiln

Ember can keep shuffle outputs in [Kiln](https://github.com/agentic-data-infra/kiln)
instead of on the workers, so that they survive a worker. How to build and
start a Kiln cluster, on one machine or many, is in
[Kiln's README](https://github.com/agentic-data-infra/kiln#readme). Point
Ember at its metadata server with `ember.shuffle.store=kiln` and
`ember.shuffle.kiln.metadata=<kiln metadata host:port>`; the examples take
extra config from `$EMBER_CONF`:

```
EMBER_CONF="ember.shuffle.store=kiln ember.shuffle.kiln.metadata=localhost:7470" \
  buck2 run //examples:word_count -- path/to/text
```

The user guide covers the rest: reading and writing
[datasets in Kiln](docs/user-guide.md#reading-data), reuse across programs
([`ember.reuse.kiln`](docs/user-guide.md#reuse-and-caching)), and
[RDMA](docs/user-guide.md#shuffle).

### Running on Kubernetes

`deploy/` runs Ember on Kubernetes from one container image
(`deploy/build_image.sh`, TCP only): Kiln (`kiln-meta` and a `kiln-server`
DaemonSet), an `ember-worker` DaemonSet, and the driver as a Job (the sort
benchmark, `ember-sort`). A local cluster with [kind](https://kind.sigs.k8s.io)
(needs Docker, kind and kubectl):

```sh
deploy/build_image.sh                       # ember:dev
deploy/kind/up.sh                           # cluster, image, Kiln, workers
deploy/kubernetes/sort_demo.sh 10000000     # 10M records (1 GB) per node
deploy/kubernetes/sort_demo.sh --kiln       # input, shuffle and output in Kiln
```

See [deploy/README.md](deploy/README.md).

## Layout

See [ember/README.md](ember/README.md).
