# shellcheck shell=bash disable=SC2154  # sourced by kind/run_cell.sh, which sets sys, size, parts, in_memory and nodes
# Sourced by run_cell.sh for ember and ember-pp: deploy and run_driver, for the cell's size, parts and in_memory.
# In memory, a node gets 3 Kiln DRAM regions of 2 GiB and 8.8 GiB for the worker; spilling, 1 GiB of Kiln DRAM, 4 localfs
# regions of 2 GiB and a 2 GiB worker.
if (( in_memory )); then
  regions=3 region_mb=2048 disk_regions=0 worker_mem=9448928051
else
  regions=1 region_mb=1024 disk_regions=4 worker_mem=$((2 << 30))
fi

deploy() {
  # Kiln's DRAM regions live in the node's /dev/shm.
  for n in "${nodes[@]}"; do docker exec "$n" mount -o remount,size=$(( regions * region_mb + 512 ))m /dev/shm; done
  k rollout restart deployment/kiln-meta > /dev/null && k rollout status deployment/kiln-meta > /dev/null
  sed -e "s|WORKER_MEM|$worker_mem|" -e "s|--regions=REGIONS|--regions=$regions|" -e "s|REGION_MB|$region_mb|" \
    "$BENCH/kind/ember.yaml" > "$L/system.yaml"
  if (( disk_regions > 0 )); then
    sed "s|DISK_REGIONS|$disk_regions|" "$BENCH/kind/kiln-disk.yaml" >> "$L/system.yaml"
  fi
  k apply -f "$L/system.yaml" > /dev/null
  k rollout status daemonset/kiln-server > /dev/null
  (( disk_regions > 0 )) && k rollout status daemonset/kiln-disk > /dev/null
  k rollout status daemonset/ember-worker > /dev/null
}

run_driver() {
  local tier=dram sample=false
  (( disk_regions > 0 )) && tier=dram+localfs
  # As on one worker: Parquet keys are sampled at 128 partitions, gensort keys are uniform.
  [[ $sys == ember-pp && $parts == 128 ]] && sample=true
  local conf="ember.cluster.workers=${#nodes[@]} ember.sort.range_partitions=$parts ember.sort.sample=$sample
    ember.shuffle.store=kiln ember.shuffle.kiln.metadata=kiln-meta.ember.svc.cluster.local:7470 ember.shuffle.kiln.tier=$tier"
  conf=$(echo $conf)
  if [[ $sys == ember ]]; then
    run_job ember:dev "[sort_benchmark, /sortbench/in$size, /bench-out/$sys, 'ember://0.0.0.0:7077', '${#nodes[@]}']" "$conf"
  else
    run_job ember-py:dev "[python3, /opt/bench/parquet_sort.py, /sortbench/pq$size, /bench-out/$sys, 'ember://0.0.0.0:7077']" "$conf"
  fi
}
