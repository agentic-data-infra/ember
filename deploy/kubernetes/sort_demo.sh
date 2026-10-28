#!/usr/bin/env bash
# The sort benchmark on a Kubernetes cluster running kiln.yaml and
# ember-worker.yaml: writes gensort input on every worker node, runs the
# sort-job.yaml driver and checks the part files on all nodes with valsort.
#
#   deploy/kubernetes/sort_demo.sh [records per node]     # default 1000000
set -euo pipefail

records=${1:-1000000}
dir=$(cd "$(dirname "$0")" && pwd)
k() { kubectl -n ember "$@"; }
pods=$(k get pods -l app=ember-worker -o name | sort)

# Every node gets its own slice of one gensort run, as 4 files.
i=0
for pod in $pods; do
  k exec "$pod" -- sh -c "rm -rf /data/sort-input /data/sorted &&
    gensort.sh generate $records /data/sort-input 4 $((i * records))"
  i=$((i + 1))
done
echo "Wrote $((i * records)) records on $i nodes"

k delete job ember-sort --ignore-not-found --wait
k apply -f "$dir/sort-job.yaml"
until [[ -n $(k get job ember-sort -o jsonpath='{.status.succeeded}{.status.failed}') ]]; do
  sleep 2
done
k logs job/ember-sort | grep "^Sorted"
[[ $(k get job ember-sort -o jsonpath='{.status.succeeded}') == 1 ]] ||
  { echo "FAIL: the driver failed" >&2; exit 1; }

# valsort summaries (-o) of every file, gathered here. The output's part
# files are in sort order by name across nodes; the input's order does not
# matter, only its record count and checksum.
sums=$(mktemp -d)
trap 'rm -rf "$sums"' EXIT
mkdir "$sums/output" "$sums/input"
for pod in $pods; do
  node=${pod#pod/}
  k exec "$pod" -- sh -c "mkdir -p /tmp/sums && rm -f /tmp/sums/* &&
    for f in /data/sorted/part-*; do
      [ -e \"\$f\" ] && valsort -o /tmp/sums/\$(basename \$f) \$f >/dev/null 2>&1
    done; tar c -C /tmp/sums ." | tar x -C "$sums/output"
  k exec "$pod" -- sh -c "rm -f /tmp/sums/* &&
    for f in /data/sort-input/part-*; do
      valsort -o /tmp/sums/$node-\$(basename \$f) \$f >/dev/null 2>&1 || true
    done; tar c -C /tmp/sums ." | tar x -C "$sums/input"
done

# Prints "<records> <checksum>" of summaries in name order; fails unless the
# records are sorted.
summary() {
  local report status=0
  report=$(cat "$1"/* | k exec -i "${pods%%$'\n'*}" -- sh -c \
    'cat > /tmp/all.sum && valsort -s /tmp/all.sum' 2>&1) || status=$?
  echo "$(awk '/^Records:/ {print $2}' <<<"$report") $(awk '/^Checksum:/ {print $2}' <<<"$report")"
  [[ $status -eq 0 ]] || { echo "$report" >&2; return 1; }
}
output=$(summary "$sums/output") || { echo "FAIL: output is not sorted" >&2; exit 1; }
input=$(summary "$sums/input" 2>/dev/null || true)
echo "output: records checksum = $output"
echo "input:  records checksum = $input"
[[ "$input" == "$output" ]] || { echo "FAIL: records differ" >&2; exit 1; }
echo "SUCCESS"
