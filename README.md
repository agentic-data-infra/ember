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
- ✅ RDMA-based shuffle: map outputs in Kiln outlive their workers and move over one-sided RDMA (`ember.shuffle.store=kiln`, see [Kiln over RDMA](#kiln-over-rdma)).
- ✅ Input and output datasets in Kiln: `kiln://<metadata>/data/<dataset>/` paths (see [Datasets in Kiln](#datasets-in-kiln)).
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
upstream Axiom plus the commits of the branch `ember`, each a candidate upstream
pull request, and Velox at the commit Axiom's `velox` submodule pins. The
Axiom version is set at the top of `scripts/build_deps.sh`. To update:

```
cd ~/source/axiom
git fetch https://github.com/facebookincubator/axiom.git main
git rebase FETCH_HEAD ember
git tag ember-YYYYMMDD
git push --force-with-lease origin ember ember-YYYYMMDD
git ls-tree HEAD velox      # the Velox commit
```

Then set `AXIOM_REF`, check out the Velox commit in `~/source/velox`, run
`scripts/build_deps.sh` and `buck2 clean`, and run the tests. Old tags stay, so
that older Ember commits still build.

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

`toArrow()` returns a pyarrow Table and `explain()` prints the plan.
`examples/word_count.py` and `examples/threshold_sweep.py` are the Python
versions of the C++ examples, and `examples/check_python_examples.py`
checks that they print the same results:

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

#### Datasets in Kiln

Programs can also read their input from Kiln datasets (Kiln's catalog: a
name pointing at a manifest of named files) and write their output as new
ones, with paths `kiln://<metadata>/data/<dataset>/`; with no `<metadata>`
(`kiln:///data/<dataset>/`) they use the cluster of `ember.kiln.metadata`, or
else of `ember.shuffle.kiln.metadata`:

```bash
third-party/kiln/scripts/start_local_cluster.sh -d 1 -l 1 -r 4   # prints localhost:7470
buck2 run kiln//bin:kiln -- --metadata=localhost:7470 data put ~/data/sort-input sort-input
EMBER_CONF="ember.kiln.metadata=localhost:7470" \
  buck2 run //examples:sort_benchmark -- kiln:///data/sort-input/ kiln:///data/sorted
buck2 run kiln//bin:kiln -- --metadata=localhost:7470 data ls sorted
```

`write().parquet(path)` writes the rows as Parquet files, to a local or
shared directory or to a Kiln dataset alike; `read().parquet(path)` reads
them back, and `write().gensort(path)` does the same with gensort records.
`write()` to a dataset that exists already fails. Each task writes its
part as one chunk, in Kiln's DRAM tier by default (`ember.kiln.files.tier=localfs`
for local disks, `ember.kiln.files.write_through=localfs` to keep a copy
there), into a bucket of the job's; the driver publishes the dataset when the
job succeeds, with all its parts, and deletes the bucket when it fails. See
[ember/common/KilnFiles.h](ember/common/KilnFiles.h).

#### Kiln over RDMA

Kiln can move shuffle data and its metadata calls over RDMA; Ember just
passes Kiln's metadata URL list through. RDMA is a build option, off by
default: the default build does not depend on rdma-core or
[rlib](https://github.com/agentic-data-infra/rlib). To build with it, link an
rlib checkout into the Kiln submodule (it becomes the Buck cell `rlib//`) and
pass `-c kiln.rdma=true`, which the `kiln//` cell reads:

```
third-party/kiln/scripts/link_rlib.sh ~/source/rlib   # needs libibverbs-dev, librdmacm-dev
buck2 build -c kiln.rdma=true //...
buck2 test -c kiln.rdma=true //...
```

(`buck2 --isolation-dir rdma ...` keeps a second build tree, so switching
modes does not rebuild everything.) Then start Kiln with RDMA
(`kiln_meta --rdma`, `kiln_server --rdma`, or
`third-party/kiln/scripts/start_local_cluster.sh -R`) and give Ember the URL
list kiln_meta prints:

```
EMBER_CONF="ember.shuffle.store=kiln ember.shuffle.kiln.metadata=tcp://meta:7470,rdma://10.0.0.1:7471" \
  word_count path/to/text ember://driver-host:7077 3
```

Clients use shared memory for storage servers on their own host and RDMA for
the others; without an RDMA device, or in a default build, everything goes
over TCP. See Kiln's README for Soft-RoCE (no RDMA NIC) and the transport
rules. In an RDMA build on a host with an RDMA device, `kiln_shuffle_test`
checks that reads and writes go over RDMA without copies, against a cluster
of its own with shared memory off (`shuffle::setKilnUseShm(false)`). With
`$EMBER_TEST_KILN_METADATA`, `kiln_shuffle_test` and `scheduler_test` run
their other tests against an external cluster.

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

`examples/SortBenchmark.cpp` sorts the 100-byte records of
[sortbenchmark.org](https://sortbenchmark.org)'s gensort by their 10-byte key
(`read().gensort(path)`: binary `key` and `payload` columns) and writes them
back as files, `write().gensort(path)`, without passing through the driver. It
runs as one merge task, or as N range tasks with
`ember.sort.range_partitions=N`. Range sorts sample the key to pick their split
points, which reads the input one extra time; gensort's keys are uniform, so the
benchmark sets `ember.sort.sample=false` and uses fixed ranges. `scripts/gensort.sh`
downloads gensort and valsort, writes the input and checks the output:

```
scripts/gensort.sh generate 10000000 /data/sort-input 8     # 1 GB in 8 files
buck2 run //examples:sort_benchmark -- /data/sort-input /data/sorted
scripts/gensort.sh validate /data/sorted /data/sort-input
```

To run it on Kubernetes, see [Running on Kubernetes](#running-on-kubernetes).

## Layout

See [ember/README.md](ember/README.md).
