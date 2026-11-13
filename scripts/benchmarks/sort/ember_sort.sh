#!/usr/bin/env bash
# One Ember sort on this machine: starts Kiln (metadata server and storage), the driver and one worker with 16 slots,
# and stops them all when the driver is done.
#   ember_sort.sh <ember|ember-pp> <size> <out> <logdir>
# ember sorts gensort records with sort_benchmark; ember-pp sorts Parquet into Parquet with parquet_sort.py.
# run_cell.sh sets the cell: PARTS (range partitions), BUDGET_GIB (unset: in memory), WHOLE_FILES=1 (one map task per
# input file), SCRATCH (shuffle and Kiln files). Binaries come from EMBER_BIN, a directory with ember_worker,
# sort_benchmark, kiln_meta and kiln_server; the Python package from EMBER_PY (default: the repo's python/), run by
# PYTHON (default: the repo's .venv).
set -uo pipefail
sys=$1 size=$2 out=$3 L=$4
source "$(dirname "$0")/lib.sh"
bin=${EMBER_BIN:?EMBER_BIN: directory with ember_worker, sort_benchmark, kiln_meta and kiln_server}

# Memory: in memory, Kiln DRAM sized to the input (2 GiB regions) and no worker cap; with a budget, 4 GiB of Kiln DRAM
# plus a localfs tier, and the rest for the worker.
if [[ -z ${BUDGET_GIB:-} ]]; then
  declare -A dram=([1g]=1 [5g]=3 [20g]=12)
  tier=dram regions=${dram[$size]} worker_mem=()
else
  tier=dram+localfs regions=2 worker_mem=(--memory_bytes=$(( (BUDGET_GIB - 4) << 30 )))
fi
# Parquet keys are sampled for the range bounds at 128 partitions; gensort keys are uniform, so no sampling there.
sample=false
[[ $sys == ember-pp && $PARTS == 128 ]] && sample=true
conf="ember.files.node_local=true ember.cluster.workers=1 ember.sort.range_partitions=$PARTS ember.sort.sample=$sample
  ember.shuffle.store=kiln ember.shuffle.kiln.metadata=localhost:7470 ember.shuffle.kiln.tier=$tier"
[[ ${WHOLE_FILES:-0} == 1 ]] && conf="$conf ember.files.max_split_bytes=2147483648"

wait_for() { # <log> <pattern>
  for _ in $(seq 1500); do grep -q "$2" "$1" 2>/dev/null && return; sleep 0.02; done
  echo "timed out waiting for '$2' in $1" >&2
}
"$bin/kiln_meta" --port=7470 > "$L/meta.log" 2>&1 &
wait_for "$L/meta.log" "Metadata server at"
"$bin/kiln_server" --metadata=localhost:7470 --port=7480 --host=localhost --regions="$regions" --region_size_mb=2048 \
  > "$L/storage.log" 2>&1 &
wait_for "$L/storage.log" "Storage server .* serves"
if [[ $tier == *localfs* ]]; then
  mkdir -p "$SCRATCH/kiln-localfs"
  "$bin/kiln_server" --metadata=localhost:7470 --port=7481 --host=localhost --tier=localfs --dir="$SCRATCH/kiln-localfs" \
    --regions=16 --region_size_mb=2048 > "$L/storage-disk.log" 2>&1 &
  wait_for "$L/storage-disk.log" "Storage server .* serves"
fi
if [[ $sys == ember ]]; then
  EMBER_CONF=$conf "$bin/sort_benchmark" "$DATA/in$size" "$out" ember://0.0.0.0:7077 1 > "$L/driver.log" 2>&1 &
else
  EMBER_CONF=$conf PYTHONPATH=${EMBER_PY:-$REPO/python} ${PYTHON:-$REPO/.venv/bin/python} "$BENCH/parquet_sort.py" \
    "$DATA/pq$size" "$out" ember://0.0.0.0:7077 > "$L/driver.log" 2>&1 &
fi
driver=$!
wait_for "$L/driver.log" "listening on port 7077"
"$bin/ember_worker" --driver=localhost:7077 --port=7078 --host=localhost --slots=$SLOTS --local_dir="$SCRATCH/ember" \
  "${worker_mem[@]}" > "$L/worker.log" 2>&1 &
worker=$!
wait $driver
rc=$?
wait $worker
jobs -p | xargs -r kill 2>/dev/null
wait 2>/dev/null
exit $rc
