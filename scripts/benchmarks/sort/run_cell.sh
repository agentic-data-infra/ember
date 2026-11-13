#!/usr/bin/env bash
# One measured sort on this machine, a single worker with 16 slots:
#   run_cell.sh <system> <cell> <logdir> [validate=1]
# Systems: ember (gensort records), ember-pp (Parquet to Parquet), and those of baselines/sort.sh.
# Cells, 20 GB unless named otherwise. The cap is the whole run's cgroup memory.max; the budget is what the system is
# configured to use, the rest of the cap is headroom (Ember: 4 GiB of Kiln DRAM, the rest for the worker):
#   mem-1g, mem-5g, mem-20g   in memory, 128 partitions, cap 44G
#   spill16-8g                16 partitions, cap 16G, budget 8 GiB
#   spill16, spill128         16 or 128 partitions, cap 16G, budget 12 GiB
#   spill128-wf               as spill128, with one map task per input file (1.25 GB each)
# Writes <logdir>/metrics: those of measure.sh, disk_read_mb and disk_write_mb (the disk of $SORTBENCH_DATA), job_ms
# (without start-up) and valid_rc. The output is deleted afterwards, unless it failed validation.
set -u
sys=$1 cell=$2 L=$3 val=${4:-1}
source "$(dirname "$0")/lib.sh"
budget='' whole_files=0
case $cell in
  mem-1g|mem-5g|mem-20g) size=${cell#mem-} parts=128 cap=44G ;;
  spill16-8g) size=20g parts=16 cap=16G budget=8 ;;
  spill16|spill128) size=20g parts=${cell#spill} cap=16G budget=12 ;;
  spill128-wf) size=20g parts=128 cap=16G budget=12 whole_files=1 ;;
  *) echo "unknown cell $cell" >&2; exit 2 ;;
esac
export SORTBENCH_DATA=$DATA PARTS=$parts BUDGET_GIB=$budget WHOLE_FILES=$whole_files SCRATCH=$DATA/scratch
runner=$BENCH/ember_sort.sh
[[ $sys == ember* ]] || runner=$BENCH/baselines/sort.sh
out=$DATA/out/$sys
rm -rf "$L" "$out" "$SCRATCH"
mkdir -p "$L" "$DATA/out" "$SCRATCH"
sync
read -r r0 w0 < <(disk_bytes)
"$BENCH/measure.sh" "$cap" "$L" "$runner" "$sys" "$size" "$out" "$L"
[[ -f $L/metrics ]] || echo "rc=lost" > "$L/metrics"
rm -rf "$SCRATCH"
sync
read -r r1 w1 < <(disk_bytes)
{ echo "disk_read_mb=$(( (r1 - r0) / 1048576 ))"; echo "disk_write_mb=$(( (w1 - w0) / 1048576 ))"
  echo "job_ms=$(job_ms "$L/driver.log")"; } >> "$L/metrics"
if [[ $val == 1 ]]; then
  validate "$out" "$size" > "$L/valid.log" 2>&1
  echo "valid_rc=$?" >> "$L/metrics"
fi
grep -q '^valid_rc=[^0]' "$L/metrics" || rm -rf "$out"
echo "$(basename "$L") $(date -u +%T) $(tr '\n' ' ' < "$L/metrics")"
