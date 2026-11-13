#!/usr/bin/env bash
# Writes the sort inputs for each size (1g, 5g or 20g) into $SORTBENCH_DATA: in<size>, 16 gensort files of 100-byte
# records, and pq<size>, the same records as 16 Parquet files (to_parquet.py).
#   gen_data.sh <size>...
set -euo pipefail
source "$(dirname "$0")/lib.sh"
for size in "$@"; do
  [[ -n ${RECORDS[$size]:-} ]] || { echo "unknown size $size (1g, 5g or 20g)" >&2; exit 2; }
  rm -rf "$DATA/in$size" "$DATA/pq$size"
  "$REPO/scripts/gensort.sh" generate "${RECORDS[$size]}" "$DATA/in$size" 16
  uv run -q --with pyarrow --with numpy python -I "$BENCH/to_parquet.py" "$DATA/in$size" "$DATA/pq$size"
done
