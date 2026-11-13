#!/usr/bin/env bash
# Builds bench-spark:1 and bench-ray:1 (Dockerfile.*) and loads them into the kind cluster (kind/up.sh). Needs SPARK_HOME,
# GLUTEN_JAR and $SORTBENCH_DATA/sortbench.jar, as baselines/sort.sh.
set -euo pipefail
source "$(dirname "$0")/../../lib.sh"
D=$BENCH/baselines/kind
stage=$(mktemp -d)
trap 'rm -rf "$stage"' EXIT
cp -r "$SPARK_HOME" "$stage/spark"
cp "$GLUTEN_JAR" "$stage/gluten.jar"
cp "$DATA/sortbench.jar" "$BENCH/baselines/ray_sort.py" "$stage/"
docker build -q -t bench-spark:1 -f "$D/Dockerfile.spark" "$stage"
docker build -q -t bench-ray:1 -f "$D/Dockerfile.ray" "$stage"
kind load docker-image --name ember bench-spark:1 bench-ray:1
