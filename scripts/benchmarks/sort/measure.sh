#!/usr/bin/env bash
# Runs a command in its own systemd user scope, capped at <cap> (cgroup memory.max, e.g. 16G), and writes to
# <logdir>/metrics: rc, wall_s, cpu_s, mem_peak_mb, anon_shmem_peak_mb (sampled every 20 ms), oom_kills and mem_max.
#   measure.sh <cap> <logdir> <cmd...>
# systemd may stop the whole scope on an OOM kill; run_cell.sh then records rc=lost.
cap=$1 L=$2
shift 2
mkdir -p "$L"
exec systemd-run --user --scope -q -p MemoryAccounting=yes -p MemoryMax="$cap" bash -c '
L=$1; shift
cg=/sys/fs/cgroup$(cut -d: -f3 /proc/self/cgroup)
( m=0; while :; do v=$(awk "/^(anon|shmem) /{s+=\$2} END{print s}" $cg/memory.stat); ((v>m)) && { m=$v; echo $m > $L/.anonpeak; }; sleep 0.02; done ) & sp=$!
t0=$(date +%s.%N)
"$@"; rc=$?
t1=$(date +%s.%N)
kill $sp 2>/dev/null; wait $sp 2>/dev/null
{ echo "rc=$rc"; echo "wall_s=$(echo "$t1 - $t0" | bc)"
  echo "cpu_s=$(awk "/^usage_usec/ {printf \"%.2f\", \$2 / 1e6}" $cg/cpu.stat)"
  echo "mem_peak_mb=$(( $(cat $cg/memory.peak) / 1048576 ))"
  echo "anon_shmem_peak_mb=$(( $(cat $L/.anonpeak 2>/dev/null || echo 0) / 1048576 ))"
  echo "oom_kills=$(awk "/^oom_kill / {print \$2}" $cg/memory.events)"; echo "mem_max=$(cat $cg/memory.max)"; } > $L/metrics
' measure "$L" "$@"
