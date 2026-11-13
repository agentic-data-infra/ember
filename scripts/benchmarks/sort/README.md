# Sort benchmark

Sorts 100-byte records (10-byte key, 90-byte payload) from
[gensort](http://www.ordinal.com/gensort.html), on one worker or on a 4-worker
[kind](https://kind.sigs.k8s.io) cluster, with Kiln as the shuffle store.
Two Ember systems:

- `ember`: gensort records in and out (`sort_benchmark`).
- `ember-pp`: Parquet in and out (`parquet_sort.py`).

`baselines/` runs the same cells on other engines (`baselines/README.md`).

## Setup

Everything a run reads or writes lives under `SORTBENCH_DATA` (default
`~/data/sortbench`): the inputs, the outputs and scratch space, so it needs
about three times the input size free. Generate the inputs once:

```
scripts/benchmarks/sort/gen_data.sh 1g 5g 20g
```

This writes `in<size>/` (16 gensort files, via `scripts/gensort.sh`) and
`pq<size>/` (the same records as 16 snappy Parquet files).

Build the binaries and the Python module, and copy the binaries into a
directory of their own, so that a long queue keeps running one build:

```
buck2 build //bin:ember_worker //examples:sort_benchmark kiln//bin:kiln_meta kiln//bin:kiln_server --show-full-output
buck2 build //python:_ember --out python/ember/_ember.so
export EMBER_BIN=<directory with ember_worker, sort_benchmark, kiln_meta, kiln_server>
```

`ember-pp` loads the module from `EMBER_PY` (default `python/`) with
`PYTHON` (default `.venv/bin/python`). Rebuild both together: a stale module
runs an old driver.

## Single worker

One worker with 16 slots (the cells assume a 16-core machine) and a Kiln
storage server, all on this machine, in one systemd scope capped by the cell
(`measure.sh`):

| cell | input | partitions | cap | budget |
|---|---|---|---|---|
| `mem-1g`, `mem-5g`, `mem-20g` | 1, 5, 20 GB | 128 | 44G | in memory: Kiln DRAM sized to the input, no worker limit |
| `spill16-8g` | 20 GB | 16 | 16G | 8 GiB: 4 GiB worker + 4 GiB Kiln DRAM |
| `spill16`, `spill128` | 20 GB | 16, 128 | 16G | 12 GiB: 8 GiB worker + 4 GiB Kiln DRAM |
| `spill128-wf` | 20 GB | 128 | 16G | as `spill128`, one map task per input file |

The cap is the cgroup `memory.max` of the whole run; the budget is what the
system is told it may use. In the spilling cells Kiln adds a localfs tier
(16 regions of 2 GiB) behind its DRAM. `ember-pp` samples keys for the range
bounds at 128 partitions; gensort keys are uniform, so `ember` never samples.

```
scripts/benchmarks/sort/run_cell.sh ember spill16 /tmp/r1          # one run
scripts/benchmarks/sort/queue.sh scripts/benchmarks/sort/run_cell.sh results/$(date +%F) \
  < scripts/benchmarks/sort/cells.txt                              # the whole matrix
```

Each run writes its logs and `metrics` (exit code, wall and job time, CPU,
peak memory, OOM kills, disk bytes, validation) to its directory, and checks
its output with valsort (gensort) or `check_parquet.py` (Parquet).

## Kind, 4 workers

The single-worker cells split four ways: each of 4 kind nodes runs a worker
with 4 slots and its own Kiln servers, capped with `docker update`. Drivers
run on the control plane; all nodes mount the inputs at `/sortbench` and a
shared output directory (`$SORTBENCH_DATA/kind-out`) at `/bench-out`.

| cell | input | partitions | node cap | per node |
|---|---|---|---|---|
| `mem` | 20 GB | 128 | 11.5 GiB | 6 GiB Kiln DRAM, 8.8 GiB worker |
| `spill16`, `spill128` | 20 GB | 16, 128 | 4.5 GiB | 1 GiB Kiln DRAM + 4 localfs regions, 2 GiB worker |
| `smoke` | 1 GB | 128 | 11.5 GiB | as `mem`, to check the cluster |

```
deploy/build_image.sh                       # ember:dev
scripts/benchmarks/sort/kind/up.sh          # recreates the cluster "ember"
scripts/benchmarks/sort/queue.sh scripts/benchmarks/sort/kind/run_cell.sh results/$(date +%F)-kind \
  < scripts/benchmarks/sort/kind/cells.txt
```

Every run tears down what ran before and deploys its system fresh. It needs
sudo, for `dmesg` (OOM kills) and to take over the root-owned output.

## Rules

- **One run at a time on the machine.** Runs measure the whole host's disk,
  and the caps assume nothing else needs the memory. `queue.sh` runs its list
  strictly in order; do not start two queues, or a queue next to anything
  heavy.
- `queue.sh` runs `hostwatch.sh` beside it, which logs host memory and stops
  the measured scope if MemAvailable drops below 6 GiB (`HOSTWATCH_MIN_MIB`),
  ending the queue.
- A failed, invalid or OOM-killed run ends its line of the cell list.
- Outputs are deleted after each run unless they failed validation; delete
  those once looked into, or the disk fills.
