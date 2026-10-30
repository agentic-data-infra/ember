#!/usr/bin/env bash
# The sort benchmark on a Kubernetes cluster running kiln.yaml and
# ember-worker.yaml: writes gensort input on every worker node, runs the
# sort-job.yaml driver and checks the part files on all nodes with valsort.
#
# With --kiln, the input, the shuffle and the output are all in Kiln: the
# input is put into Kiln as dataset sort-input, the driver sorts it into a
# new dataset, sorted, and the check reads that back from Kiln.
#
#   deploy/kubernetes/sort_demo.sh [--kiln] [records per node]  # default 1000000
set -euo pipefail

kiln=false
if [[ ${1:-} == --kiln ]]; then
  kiln=true
  shift
fi
records=${1:-1000000}
dir=$(cd "$(dirname "$0")" && pwd)
k() { kubectl -n ember "$@"; }
pods=$(k get pods -l app=ember-worker -o name | sort)
nodes=$(wc -l <<<"$pods")
first=${pods%%$'\n'*}

# Runs the driver Job (sort-job.yaml, through sed $1); fails if it fails.
run_driver() {
  k delete job ember-sort --ignore-not-found --wait
  sed "$1" "$dir/sort-job.yaml" | k apply -f -
  until [[ -n $(k get job ember-sort -o jsonpath='{.status.succeeded}{.status.failed}') ]]; do
    sleep 2
  done
  k logs job/ember-sort | grep "^Sorted"
  [[ $(k get job ember-sort -o jsonpath='{.status.succeeded}') == 1 ]] ||
    { echo "FAIL: the driver failed" >&2; exit 1; }
}

if $kiln; then
  # One node writes the whole gensort run, 4 files per node, and puts it
  # into Kiln; the check later reads the output back next to it.
  kiln_cli="kiln --metadata=kiln-meta.ember.svc.cluster.local:7470"
  k exec "$first" -- sh -c "rm -rf /data/kiln-sort &&
    gensort.sh generate $((nodes * records)) /data/kiln-sort/input $((nodes * 4)) &&
    { $kiln_cli data rm sort-input; $kiln_cli data rm sorted; } >/dev/null 2>&1;
    $kiln_cli data put /data/kiln-sort/input sort-input"
  echo "Put $((nodes * records)) records into Kiln dataset sort-input"

  # Any worker reads any input file from Kiln: no node-local splits.
  run_driver 's|value: /data/sort-input|value: kiln:///data/sort-input/|
    s|value: /data/sorted|value: kiln:///data/sorted|
    /ember.files.node_local=true/d'

  k exec "$first" -- sh -c "$kiln_cli data ls sorted | head -n 3 && echo ... &&
    $kiln_cli data get sorted /data/kiln-sort/sorted >/dev/null &&
    gensort.sh validate /data/kiln-sort/sorted /data/kiln-sort/input &&
    rm -rf /data/kiln-sort"
  exit
fi

# Every node gets its own slice of one gensort run, as 4 files.
i=0
for pod in $pods; do
  k exec "$pod" -- sh -c "rm -rf /data/sort-input /data/sorted &&
    gensort.sh generate $records /data/sort-input 4 $((i * records))"
  i=$((i + 1))
done
echo "Wrote $((i * records)) records on $i nodes"

run_driver ""

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
  report=$(cat "$1"/* | k exec -i "$first" -- sh -c \
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
