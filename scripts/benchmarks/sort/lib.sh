# shellcheck shell=bash disable=SC2034  # sourced
# Sourced by the sort benchmark scripts: paths, input sizes and the parts of a run's metrics that every system shares.
# SORTBENCH_DATA (default ~/data/sortbench) holds the inputs (in<size>: gensort, pq<size>: Parquet), the outputs and
# the scratch space of single-worker runs, and kind-out/, the kind cluster's output directory.
BENCH=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO=$(cd "$BENCH/../../.." && pwd)
DATA=${SORTBENCH_DATA:-$HOME/data/sortbench}
SLOTS=16 # the cells assume a 16-core machine
declare -A RECORDS=([1g]=10000000 [5g]=50000000 [20g]=200000000)

# Bytes read and written so far on the disk that holds $DATA.
disk_bytes() {
  local dev
  dev=$(basename "$(df --output=source "$DATA" | tail -1)")
  awk -v d="$dev" '$3 == d {print $6 * 512, $10 * 512}' /proc/diskstats
}

# job_ms <driver log>: the job time the driver printed, without start-up ("Sorted ... in <ms> ms" or "JOBTIME_MS <ms>").
job_ms() {
  sed -nE 's/^Sorted .* in ([0-9]+) ms.*/\1/p; s/^JOBTIME_MS ([0-9]+)$/\1/p' "$1" | tail -1
}

# validate <output dir> <size>: whether the output holds the input's records in order; Parquet or gensort records.
validate() {
  if compgen -G "$1/*.parquet" >/dev/null; then
    uv run -q --with pyarrow --with numpy python -I "$BENCH/check_parquet.py" "$1" "${RECORDS[$2]}" | tee /dev/stderr |
      grep -q VALID=yes
  else
    "$REPO/scripts/gensort.sh" validate "$1" "$DATA/in$2"
  fi
}
