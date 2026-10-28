#!/usr/bin/env bash
# Creates the kind cluster, loads the Ember image into it and starts Kiln and
# the Ember workers.
#
#   deploy/kind/up.sh [tag]      # default ember:dev, built by build_image.sh
set -euo pipefail

tag=${1:-ember:dev}
dir=$(cd "$(dirname "$0")" && pwd)

kind get clusters | grep -qx ember || kind create cluster --config "$dir/cluster.yaml"
kind load docker-image --name ember "$tag"
# Kiln's DRAM regions live in the node's /dev/shm, which Docker caps at 64 MB.
for node in $(kind get nodes --name ember); do
  docker exec "$node" mount -o remount,size=8g /dev/shm
done
kubectl apply -f "$dir/../kubernetes/namespace.yaml"
kubectl apply -f "$dir/../kubernetes/kiln.yaml" -f "$dir/../kubernetes/ember-worker.yaml"
kubectl -n ember rollout status deployment/kiln-meta
kubectl -n ember rollout status daemonset/kiln-server
kubectl -n ember rollout status daemonset/ember-worker
