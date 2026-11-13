#!/usr/bin/env bash
# Runs a list of cells one run at a time, with hostwatch.sh logging the host throughout:
#   queue.sh <run_cell.sh or kind/run_cell.sh> <results dir> < cells.txt
# Each line of the list is "<system> <cell> <run>...", e.g. "ember spill16 warmup w1 w2"; # starts a comment. A run
# named warmup is not validated. A failed, invalid or OOM-killed run ends its line; HOSTSTOP ends the queue. Each run
# writes <results dir>/runs/<system>-<cell>-<run>/ and prints its metrics on one line.
set -u
run_cell=$(realpath "$1") R=$2
mkdir -p "$R/runs"
"$(dirname "$0")/hostwatch.sh" "$R/hostwatch.log" &
watch=$!
trap 'kill $watch 2>/dev/null' EXIT
while read -r -u 3 sys cell runs; do
  [[ -z $sys || $sys == \#* ]] && continue
  for run in ${runs%%#*}; do
    val=1
    [[ $run == warmup ]] && val=0
    L=$R/runs/$sys-$cell-$run
    "$run_cell" "$sys" "$cell" "$L" $val
    grep -q HOSTSTOP "$R/hostwatch.log" 2>/dev/null && { echo "HOSTSTOP at $sys-$cell-$run"; exit 1; }
    if ! grep -q '^rc=0$' "$L/metrics" || grep -qE '^(valid_rc|oom_kills)=[^0]' "$L/metrics"; then
      echo "FAILED $sys-$cell-$run"
      break
    fi
  done
done 3<&0 < /dev/null
echo DONE
