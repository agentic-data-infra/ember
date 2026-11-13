#!/usr/bin/env bash
# Logs the host's MemAvailable, SwapFree (MiB) and memory pressure every 5 s into <log>, until killed. Below
# HOSTWATCH_MIN_MIB (default 6144) MemAvailable, it stops every systemd user scope (the runs of measure.sh) and logs
# HOSTSTOP, so that a run that outgrows its cap cannot take the host down. queue.sh runs it.
#   hostwatch.sh <log>
log=$1 min=${HOSTWATCH_MIN_MIB:-6144}
while :; do
  avail=$(awk '/^MemAvailable/ {print int($2 / 1024)}' /proc/meminfo)
  swap=$(awk '/^SwapFree/ {print int($2 / 1024)}' /proc/meminfo)
  echo "$(date -u +%T) avail_mb=$avail swapfree_mb=$swap $(head -1 /proc/pressure/memory)" >> "$log"
  if (( avail < min )); then
    echo "$(date -u +%T) HOSTSTOP avail_mb=$avail" >> "$log"
    systemctl --user list-units --type=scope --no-legend 'run-*.scope' | awk '{print $1}' | xargs -r systemctl --user stop
  fi
  sleep 5
done
