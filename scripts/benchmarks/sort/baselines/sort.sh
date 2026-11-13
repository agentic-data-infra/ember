#!/usr/bin/env bash
# One baseline sort on this machine, 16 cores, for run_cell.sh:
#   sort.sh <rdd|df|gluten|ray> <size> <out> <logdir>
#   rdd     Spark RDD TeraSort on the gensort records (SortBench.scala), records out
#   df      Spark DataFrame orderBy("key") on the Parquet input, Parquet out (adaptive query execution on)
#   gluten  df on Gluten (Velox) with columnar shuffle
#   ray     Ray Data sort("key") on the Parquet input, Parquet out (ray_sort.py)
# The cell (run_cell.sh) sets PARTS, BUDGET_GIB (unset: in memory, Spark gets 48 GiB and Ray its defaults),
# WHOLE_FILES=1 (one task per input file) and SCRATCH. Spark gets the budget as its heap; Gluten a third as heap and the
# rest off-heap. Ray's object store gets its default 30% of the budget.
# Needs SPARK_HOME (Spark 3.5.5), GLUTEN_JAR (the Gluten 1.7.0 Velox bundle for Spark 3.5), $SORTBENCH_DATA/sortbench.jar
# (build_jar.sh) and RAY_PYTHON (default python3), a Python with ray[data] 2.60.0.
set -u
sys=$1 size=$2 out=$3 L=$4
source "$(dirname "$0")/../lib.sh"
budget=${BUDGET_GIB:-}

if [[ $sys == ray ]]; then
  # Ray puts its sockets under its temp dir, whose path must stay short.
  ln -sfn "$SCRATCH" /tmp/ray-sortbench
  RAY_TMP=/tmp/ray-sortbench RAY_BUDGET=${budget:+$((budget << 30))} timeout 3000 "${RAY_PYTHON:-python3}" -I \
    "$BENCH/baselines/ray_sort.py" "$DATA/pq$size" "$out" "$PARTS" < /dev/null > "$L/driver.log" 2>&1
  rc=$?
  rm -f /tmp/ray-sortbench
  exit $rc
fi

heap=${budget:-48}
conf=(--master "local[$SLOTS]" --conf spark.local.dir="$SCRATCH" --conf spark.ui.enabled=false
  --conf spark.eventLog.enabled=true --conf spark.eventLog.dir="file://$L"
  --conf spark.sql.adaptive.enabled=true --conf spark.sql.shuffle.partitions="$PARTS"
  --conf "spark.driver.extraJavaOptions=-Dio.netty.tryReflectionSetAccessible=true --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED")
case $sys in
  rdd)
    conf+=(--driver-memory "${heap}g")
    args=(rdd "$DATA/in$size" "$out" "$PARTS")
    [[ ${WHOLE_FILES:-0} == 1 ]] && conf+=(--conf spark.hadoop.mapreduce.input.fileinputformat.split.minsize=2147483648) ;;
  df|gluten)
    args=(df "$DATA/pq$size" "$out")
    [[ ${WHOLE_FILES:-0} == 1 ]] && conf+=(--conf spark.sql.files.maxPartitionBytes=2147483648)
    if [[ $sys == df ]]; then
      conf+=(--driver-memory "${heap}g")
    else
      onheap=$(( (heap + 2) / 3 ))
      conf+=(--driver-memory "${onheap}g" --jars "$GLUTEN_JAR" --conf spark.driver.extraClassPath="$GLUTEN_JAR"
        --conf spark.plugins=org.apache.gluten.GlutenPlugin
        --conf spark.memory.offHeap.enabled=true --conf spark.memory.offHeap.size="$(( heap - onheap ))g"
        --conf spark.shuffle.manager=org.apache.spark.shuffle.sort.ColumnarShuffleManager)
    fi ;;
  *) echo "unknown system $sys" >&2; exit 2 ;;
esac
timeout 3000 "$SPARK_HOME/bin/spark-submit" "${conf[@]}" --class SortBench "$DATA/sortbench.jar" "${args[@]}" \
  < /dev/null > "$L/driver.log" 2>&1
