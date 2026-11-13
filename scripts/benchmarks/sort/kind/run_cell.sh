#!/usr/bin/env bash
# One sort on the 4-worker kind cluster (up.sh), one system at a time:
#   run_cell.sh <system> <cell> <logdir> [validate=1]
# Systems: ember, ember-pp (kind/ember.sh), and those of baselines/kind/systems.sh.
# Cells split the single-worker cells four ways, per worker node:
#   mem               20 GB in memory, 128 partitions, node cap 11.5 GiB (Ember: 6 GiB Kiln DRAM)
#   spill16, spill128 20 GB, 16 or 128 partitions, node cap 4.5 GiB and a 3 GiB budget (Ember: 2 GiB worker, 1 GiB Kiln
#                     DRAM and a localfs tier)
#   smoke             1 GB as mem, to check the cluster
# Every run tears down whatever ran before, caps each worker node's memory (docker update), clears the nodes' scratch,
# deploys its system fresh and runs its driver on the control plane. Writes <logdir>/metrics: rc, wall_s, oom_kills
# (kernel OOM kills on the host), disk_read_mb, disk_write_mb, job_ms and valid_rc. Needs sudo for dmesg and to take
# over the root-owned output. The output is deleted afterwards, unless it failed validation.
set -u
sys=$1 cell=$2 L=$3 val=${4:-1}
source "$(dirname "$0")/../lib.sh"
k() { kubectl -n ember "$@"; }
mapfile -t nodes < <(kind get nodes --name ember | grep -v control-plane)
size=20g in_memory=1
case $cell in
  mem) parts=128 node_mem=11776m ;;
  smoke) size=1g parts=128 node_mem=11776m ;;
  spill16|spill128) parts=${cell#spill} node_mem=4608m in_memory=0 ;;
  *) echo "unknown cell $cell" >&2; exit 2 ;;
esac
out=$DATA/kind-out/$sys
rm -rf "$L"
mkdir -p "$L"

# run_job <image> <command, a YAML list> [EMBER_CONF]: runs the driver as a Job into $L/driver.log; fails if the Job did.
run_job() {
  sed -e "s|image: IMAGE|image: $1|" -e "s|command: ARGS|command: $2|" -e "s|value: \"CONF\"|value: \"${3:-}\"|" \
    "$BENCH/kind/driver.yaml" > "$L/driver.yaml"
  k apply -f "$L/driver.yaml" > /dev/null
  until [[ -n $(k get job bench-driver -o jsonpath='{.status.succeeded}{.status.failed}') ]]; do sleep 1; done
  k logs job/bench-driver > "$L/driver.log" 2>&1
  [[ $(k get job bench-driver -o jsonpath='{.status.succeeded}') == 1 ]]
}

# Each system's file defines deploy and run_driver.
if [[ $sys == ember* ]]; then source "$BENCH/kind/ember.sh"; else source "$BENCH/baselines/kind/systems.sh"; fi

# Tear down, cap and clear the nodes. Only kiln-meta (Deployment) outlives a run.
k delete job bench-driver --ignore-not-found --wait > /dev/null
k delete daemonset --all --wait > /dev/null
k delete deployment -l bench --wait > /dev/null
k wait --for=delete pod -l 'app!=kiln-meta' --timeout=180s > /dev/null 2>&1
for n in "${nodes[@]}"; do
  docker update --memory=$node_mem --memory-swap=$node_mem "$n" > /dev/null
  docker exec "$n" rm -rf /var/lib/bench-scratch
  docker exec "$n" find /dev/shm -maxdepth 1 -name 'kiln-*' -exec rm -rf {} +
done
sudo rm -rf "$out"
sync
deploy
sleep 2

oom() { sudo dmesg | grep -c 'oom-kill:'; }
o0=$(oom)
read -r r0 w0 < <(disk_bytes)
t0=$(date +%s.%N)
run_driver
rc=$?
t1=$(date +%s.%N)
sync
read -r r1 w1 < <(disk_bytes)
o1=$(oom)
for p in $(k get pods -o name); do k logs "$p" > "$L/${p#pod/}.log" 2>&1; done
{ echo "rc=$rc"; echo "wall_s=$(echo "$t1 - $t0" | bc)"; echo "oom_kills=$((o1 - o0))"
  echo "disk_read_mb=$(( (r1 - r0) / 1048576 ))"; echo "disk_write_mb=$(( (w1 - w0) / 1048576 ))"
  echo "job_ms=$(job_ms "$L/driver.log")"; } > "$L/metrics"
# The pods write as root; take the output over to validate and remove it.
sudo chown -R "$(id -u):$(id -g)" "$out" 2>/dev/null
if [[ $val == 1 ]]; then
  validate "$out" "$size" > "$L/valid.log" 2>&1
  echo "valid_rc=$?" >> "$L/metrics"
fi
grep -q '^valid_rc=[^0]' "$L/metrics" || rm -rf "$out"
echo "$(basename "$L") $(date -u +%T) $(tr '\n' ' ' < "$L/metrics")"
