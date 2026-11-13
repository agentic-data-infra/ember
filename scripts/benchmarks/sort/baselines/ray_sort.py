# One Ray Data sort of the benchmark's Parquet input on a single node, with Ray's documented defaults:
#   ray_sort.py <in dir> <out dir> <blocks: 0 = Ray's default>
# Env: RAY_TMP (Ray's temp dir, where it spills its object store); RAY_BUDGET (bytes, optional): the job's memory budget,
# of which the object store gets Ray's default share of 30%, as if the node had that much memory. Prints JOBTIME_MS: read, sort and write, without
# starting Ray, as Spark's JOBTIME_MS leaves out the JVM. With RAY_ADDRESS set, it joins that running cluster instead
# (baselines/kind/: the cluster's workers have their CPUs and object stores; RAY_TMP and RAY_BUDGET do not apply).
import os, sys, time
import ray

inp, out, blocks = sys.argv[1], sys.argv[2], int(sys.argv[3])
budget = os.environ.get("RAY_BUDGET") or None
if os.environ.get("RAY_ADDRESS"):
    ray.init(log_to_driver=False)
else:
    ray.init(num_cpus=16, _temp_dir=os.environ["RAY_TMP"], include_dashboard=False, log_to_driver=False,
             object_store_memory=int(int(budget) * 0.3) if budget else None)
print("RAY_VERSION", ray.__version__, flush=True)
t0 = time.time()
ds = ray.data.read_parquet(inp, override_num_blocks=blocks or None)
ds.sort("key").write_parquet(out)
print("JOBTIME_MS", int((time.time() - t0) * 1000), flush=True)
ray.shutdown()
