#!/usr/bin/env bash
# Runs an Ember program as a cluster on this machine: N ember-worker
# processes plus the program as the driver, started separately as they would
# be on different hosts.
#
#   scripts/run_local_cluster.sh [-n workers] [-s slots] [-p port] -- \
#       <program> <args...>
#
# The program gets the master URL in $EMBER_MASTER and the worker count in
# $EMBER_WORKERS; "{master}" and "{workers}" in its arguments are replaced by
# them. Worker logs go to $EMBER_LOG_DIR (default <tmp>/ember-logs).
#
#   scripts/run_local_cluster.sh -n 3 -- \
#       buck-out/.../word_count corpus/ {master} {workers}
set -euo pipefail

workers=2
slots=2
port=7077
while getopts "n:s:p:" opt; do
  case $opt in
    n) workers=$OPTARG ;;
    s) slots=$OPTARG ;;
    p) port=$OPTARG ;;
    *) exit 2 ;;
  esac
done
shift $((OPTIND - 1))
[[ "${1:-}" == "--" ]] && shift
[[ $# -ge 1 ]] || { echo "usage: $0 [-n workers] [-s slots] [-p port] -- program args..." >&2; exit 2; }

root=$(cd "$(dirname "$0")/.." && pwd)
worker_bin=${EMBER_WORKER:-}
if [[ -z "$worker_bin" ]]; then
  worker_bin=$(cd "$root" && buck2 build //bin:ember_worker --show-full-output 2>/dev/null | awk '{print $2}')
fi
log_dir=${EMBER_LOG_DIR:-${TMPDIR:-/tmp}/ember-logs}
mkdir -p "$log_dir"

pids=()
cleanup() { for pid in "${pids[@]}"; do kill "$pid" 2>/dev/null || true; done; }
trap cleanup EXIT

for ((i = 0; i < workers; i++)); do
  "$worker_bin" --driver="localhost:$port" --slots="$slots" --host=localhost \
    >"$log_dir/worker-$i.log" 2>&1 &
  pids+=($!)
done

export EMBER_MASTER="ember://localhost:$port"
export EMBER_WORKERS=$workers
args=()
for arg in "$@"; do
  arg=${arg//\{master\}/$EMBER_MASTER}
  args+=("${arg//\{workers\}/$EMBER_WORKERS}")
done
"${args[@]}"
