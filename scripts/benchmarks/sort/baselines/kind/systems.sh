# shellcheck shell=bash disable=SC2154  # sourced by kind/run_cell.sh, which sets sys, size, parts, in_memory and nodes
# Sourced by kind/run_cell.sh for rdd, df, gluten and ray (as in baselines/sort.sh): deploy and run_driver, for the cell's
# size, parts and in_memory. Per node, in memory: Spark an 8 GiB executor, Gluten 3 GiB heap and 6 GiB off-heap, Ray an
# object store of 30% of the 11 GiB node; spilling, 3 GiB, 1 + 2 GiB and 30% of 3 GiB.
D=$BENCH/baselines/kind
if (( in_memory )); then
  heap=8g gheap=3g goff=6g obj=3543348019
else
  heap=3g gheap=1g goff=2g obj=966367641
fi

deploy() {
  case $sys in
    rdd|df|gluten)
      cp "$D/spark.yaml" "$L/system.yaml"
      k apply -f "$L/system.yaml" > /dev/null
      k rollout status deployment/spark-master > /dev/null && k rollout status daemonset/spark-worker > /dev/null
      until [[ $(k logs deployment/spark-master 2>/dev/null | grep -c 'Registering worker') -ge ${#nodes[@]} ]]; do sleep 1; done ;;
    ray)
      sed "s|OBJECT_STORE|$obj|" "$D/ray.yaml" > "$L/system.yaml"
      k apply -f "$L/system.yaml" > /dev/null
      k rollout status deployment/ray-head > /dev/null && k rollout status daemonset/ray-worker > /dev/null
      until k exec deployment/ray-head -- env RAY_AUTH_MODE=disabled python -c 'import sys, ray; ray.init(log_to_driver=False)
sys.exit(0 if ray.cluster_resources().get("CPU", 0) >= 16 else 1)' > /dev/null 2>&1; do sleep 2; done ;;
    *) echo "unknown system $sys" >&2; exit 2 ;;
  esac
}

run_driver() {
  if [[ $sys == ray ]]; then
    k exec deployment/ray-head -- env RAY_AUTH_MODE=disabled RAY_ADDRESS=auto \
      python /opt/bench/ray_sort.py /sortbench/pq$size /bench-out/$sys $parts > "$L/driver.log" 2>&1
    return
  fi
  local conf="--master spark://spark-master.ember.svc.cluster.local:7077 --conf spark.driver.host=\$POD_IP
    --conf spark.driver.bindAddress=0.0.0.0 --conf spark.driver.memory=2g --conf spark.executor.cores=4
    --conf spark.cores.max=16 --conf spark.ui.enabled=false --conf spark.sql.adaptive.enabled=true
    --conf spark.sql.shuffle.partitions=$parts --conf spark.driver.extraJavaOptions=-Dio.netty.tryReflectionSetAccessible=true
    --conf spark.executor.extraJavaOptions=-Dio.netty.tryReflectionSetAccessible=true"
  local job="--class SortBench /opt/bench/sortbench.jar df /sortbench/pq$size /bench-out/$sys"
  case $sys in
    rdd) conf="$conf --conf spark.executor.memory=$heap"
         job="--class SortBench /opt/bench/sortbench.jar rdd /sortbench/in$size /bench-out/$sys $parts" ;;
    df) conf="$conf --conf spark.executor.memory=$heap" ;;
    gluten) conf="$conf --jars /opt/bench/gluten.jar --conf spark.driver.extraClassPath=/opt/bench/gluten.jar
      --conf spark.executor.extraClassPath=/opt/bench/gluten.jar --conf spark.plugins=org.apache.gluten.GlutenPlugin
      --conf spark.memory.offHeap.enabled=true --conf spark.memory.offHeap.size=$goff
      --conf spark.shuffle.manager=org.apache.spark.shuffle.sort.ColumnarShuffleManager --conf spark.executor.memory=$gheap" ;;
  esac
  run_job bench-spark:1 "[sh, -c, 'spark-submit $(echo $conf) $job']"
}
