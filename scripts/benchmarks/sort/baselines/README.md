# Sort benchmark baselines

The cells of `../README.md` on other engines: Spark RDD TeraSort (`rdd`),
Spark DataFrame (`df`), Gluten (`gluten`) and Ray Data (`ray`).
`run_cell.sh`, `kind/run_cell.sh` and `queue.sh` run them like the Ember
systems:

```
scripts/benchmarks/sort/queue.sh scripts/benchmarks/sort/run_cell.sh results/$(date +%F)-baselines \
  < scripts/benchmarks/sort/baselines/cells.txt
scripts/benchmarks/sort/queue.sh scripts/benchmarks/sort/kind/run_cell.sh results/$(date +%F)-kind-baselines \
  < scripts/benchmarks/sort/baselines/kind/cells.txt
```

| file | contents |
|---|---|
| `sort.sh` | one single-worker run (16 cores) for `run_cell.sh` |
| `SortBench.scala`, `build_jar.sh` | the Spark jobs, built into `$SORTBENCH_DATA/sortbench.jar` |
| `ray_sort.py` | the Ray Data job |
| `kind/` | images, manifests and `systems.sh` for `kind/run_cell.sh` |

Single worker, it needs `SPARK_HOME` (Spark 3.5.5), `GLUTEN_JAR` (Gluten
1.7.0's Velox bundle for Spark 3.5) and `RAY_PYTHON` (a Python with
`ray[data]==2.60.0`); `build_jar.sh` also `SCALA_COMPILER_JAR`. On kind,
run `kind/build_images.sh` after `../kind/up.sh`.

Memory: Spark gets the cell's budget as heap, Gluten a third of it as heap
and the rest off-heap, Ray 30% of it as object store (its default share). In
memory, Spark gets 48 GiB (Gluten 16 + 32) and Ray its defaults. On kind,
`kind/systems.sh` has the per-node sizes.
