# How Ember compares

Ember next to other distributed data processing systems, as of November
2026. Ember entries describe what is implemented today; planned work is
marked as such. Claims about other systems cite their documentation
(`[n]`, see Sources).

Legend: ✅ supported, 🟡 partial, limited or through a plugin, ⬜ planned,
❌ not available.

| | **Ember** (+ [Kiln](https://github.com/agentic-data-infra/kiln), [rlib](https://github.com/agentic-data-infra/rlib)) | **Apache Spark** | **Apache Gluten** (Spark + Velox) | **Ray Data** | **Presto** (Java) | **Prestissimo** (Presto C++ workers) |
|---|---|---|---|---|---|---|
| **Fault tolerance** | ✅ Task retry, worker loss detection, lineage recompute of lost shuffle output, stage re-attempts; shuffle in Kiln outlives workers (a) | ✅ Lineage recompute, task and stage retry, speculative execution, external shuffle service [1][2]; driver restart only with `--supervise`, the job restarts [3] | ✅ Inherits Spark's [9] | ✅ Task retry, lineage reconstruction of objects, actor restart; ❌ objects of a dead owner are lost [12] | ❌ A worker failure fails the query; 🟡 Presto on Spark [15], recoverable grouped execution [16] | As Presto; Presto on Spark can run native workers [15] |
| **GPU support** | ⬜ Planned, through Velox's cuDF backend (b) | 🟡 GPU-aware scheduling [4]; SQL/DataFrame acceleration through the RAPIDS plugin [5] | 🟡 Experimental Velox cuDF backend [10] | ✅ GPU scheduling, `map_batches(num_gpus=...)` [13]; relational operators on CPU | ❌ | 🟡 Velox cuDF, GPU nightly images [17][18] |
| **RDMA support** | ✅ Kiln moves shuffle and dataset blocks with one-sided RDMA (zero copy), metadata calls over rlib RPC; build option (c) | ❌ TCP; 🟡 plugins: RAPIDS UCX shuffle [6], SparkUCX [7] | ❌ TCP: columnar shuffle, or Celeborn/Uniffle [9][11] | 🟡 Ray Direct Transport for GPU tensors between actors [14]; not used by Ray Data's shuffle | ❌ HTTP exchange | ❌ HTTP exchange |
| **Implementation language** | C++20 throughout (Velox, Axiom, folly coroutines, fbthrift) | Scala/Java (JVM) | Java/Scala plugin, C++ backends through JNI and Substrait [9] | C++ core (raylet, GCS, object store); Ray Data in Python on Arrow | Java | C++ workers on Velox, Java coordinator |
| **Python API** | ✅ DataFrame API returning pyarrow tables (Linux x86-64 wheel, CPython 3.10) | ✅ PySpark DataFrame, SQL, pandas API, Spark Connect [8] | ✅ Unchanged PySpark | ✅ Python-first | 🟡 SQL through a DB-API client [19] | 🟡 As Presto |
| **Shuffle / intermediate store** | ✅ Files on the workers, or Kiln (tiered DRAM/local FS, optional RDMA, survives workers) | Local files + external shuffle service, push-based shuffle, remote shuffle plugins [1] | Columnar shuffle; Celeborn/Uniffle [11] | Ray object store (shared memory, spills to disk) [13] | Pipelined in-memory exchange | As Presto |
| **Reuse / memoization** | ✅ Content-hashed stages reused within a session; `cache`/`persist`; reuse across programs through Kiln's catalog. ⬜ Across sessions and agents, cost-based materialization | `cache`/`persist`, per session; exchange reuse within a query | As Spark | `materialize()`; no automatic reuse | No user-level memoization | As Presto |
| **LLM calls** | ✅ `ai.generate` in Dataframe expressions: OpenAI-compatible endpoints, typed output schemas, a failed row gets a status instead of failing the job, calls only on rows that reach them, asynchronous on Velox's RPC operator, stage reuse (d) | 🟡 Python UDFs (pandas or Arrow) that call a model [20] | As Spark | ✅ `ray.data.llm` processors: vLLM or SGLang engines in the pipeline, or hosted endpoints [21] | ❌ | 🟡 Remote scalar functions on a Thrift function server [22]; Velox's async RPC operator for inference [23] |
| **Memory / spill** | ✅ Velox memory manager and arbitrator, spill on reclaim; shuffle can spill into Kiln | Unified memory manager, spill to disk | Velox memory pools under Spark's memory manager, spill | Streaming executor with backpressure, object spilling [13] | Memory pools, spill for joins, aggregations and sorts | As Presto (Velox spill) |
| **Elasticity / autoscaling** | ⬜ Planned for every component | ✅ Dynamic allocation [2] | As Spark | ✅ Ray autoscaler, KubeRay | 🟡 No built-in autoscaler | As Presto |
| **User API** | DataFrame with SQL expressions | SQL + DataFrame | As Spark (unsupported operators fall back to the JVM) | Dataset API | SQL | SQL |
| **Deployment** | In-process (`local[*]`), standalone cluster, Kubernetes | Local, standalone, YARN, Kubernetes | As Spark | Local, VMs, KubeRay | Coordinator + workers, Kubernetes | As Presto |
| **License** | Apache 2.0 | Apache 2.0 | Apache 2.0 | Apache 2.0 | Apache 2.0 | Apache 2.0 |

## Notes on Ember

a. **Fault tolerance:** a failed task is retried up to 4 times. Workers are
   dropped on a broken connection or missed heartbeats, and a hung worker
   shows up as a fetch failure after the fetch timeout. Lost map outputs are
   recomputed recursively on the remaining workers, and a stage is attempted
   up to 4 times. See [ember/driver/README.md](../ember/driver/README.md)
   ("Lost shuffle outputs").
b. **GPU:** Velox has a cuDF backend; Ember does not build it yet and has no
   GPU scheduling or GPU shuffle.
c. **RDMA:** build Ember with `-c kiln.rdma=true` (see
   [Kiln over RDMA](../README.md#kiln-over-rdma)). RDMA covers Kiln's DRAM
   tier; Kiln's local-FS tier, the default worker shuffle and the
   driver/worker RPC use TCP. Tested so far with Soft-RoCE.
d. **LLM calls:** Ember does not host models; an endpoint is any
   OpenAI-compatible server (vLLM, SGLang, llama.cpp, hosted APIs). Cheap
   filters spare the model rows when they come before the AI column or in
   the same filter. See [ai-functions.md](ai-functions.md).

## Sources

1. Spark RDD persistence and fault tolerance: https://spark.apache.org/docs/latest/rdd-programming-guide.html#rdd-persistence
2. Spark job scheduling, dynamic allocation and external shuffle service: https://spark.apache.org/docs/latest/job-scheduling.html
3. Spark standalone `--supervise`: https://spark.apache.org/docs/latest/spark-standalone.html#launching-spark-applications
4. Spark custom resource (GPU) scheduling: https://spark.apache.org/docs/latest/configuration.html#custom-resource-scheduling-and-configuration-overview
5. RAPIDS Accelerator for Apache Spark: https://docs.nvidia.com/spark-rapids/user-guide/latest/
6. RAPIDS Shuffle Manager (UCX mode): https://docs.nvidia.com/cudf-spark/latest/additional-functionality/rapids-shuffle.html
7. SparkUCX: https://github.com/openucx/sparkucx
8. PySpark and Spark Connect: https://spark.apache.org/docs/latest/api/python/
9. Apache Gluten: https://github.com/apache/gluten ; Velox backend: https://gluten.apache.org/docs/velox/getting-started
10. Gluten Velox GPU: https://gluten.apache.org/docs/v1.5.0/get-started/VeloxGPU.html
11. Celeborn support for Gluten: https://celeborn.apache.org/docs/latest/developers/glutensupport/
12. Ray object fault tolerance: https://docs.ray.io/en/latest/ray-core/fault_tolerance/objects.html
13. Ray Data internals: https://docs.ray.io/en/latest/data/data-internals.html
14. Ray Direct Transport: https://docs.ray.io/en/latest/ray-core/direct-transport/direct-transport.html
15. Presto on Spark: https://prestodb.io/docs/current/admin/spark.html
16. Presto Unlimited (recoverable grouped execution): https://prestodb.io/blog/2019/08/05/presto-unlimited-mpp-database-at-scale/
17. GPU-accelerated Presto C++: https://prestodb.io/blog/2026/06/24/gpu-accelerated-presto-c/
18. Velox and cuDF: https://developer.nvidia.com/blog/accelerating-large-scale-data-analytics-with-gpu-native-velox-and-nvidia-cudf
19. Presto Python client: https://github.com/prestodb/presto-python-client
20. PySpark pandas and Arrow UDFs: https://spark.apache.org/docs/latest/api/python/tutorial/sql/arrow_pandas.html
21. Ray Data, working with LLMs: https://docs.ray.io/en/latest/data/working-with-llms.html
22. Presto C++ remote functions: https://prestodb.io/docs/current/presto_cpp/features.html
23. Velox async RPC design: https://github.com/facebookincubator/velox/blob/main/velox/docs/designs/async-rpc.md
