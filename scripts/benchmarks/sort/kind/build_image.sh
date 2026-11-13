#!/usr/bin/env bash
# Builds ember-py:dev, ember:dev plus the Python package and parquet_sort.py, and loads it into the kind cluster.
# EMBER_PY: the directory holding the ember package, built from ember:dev's commit (default: the repo's python/).
set -euo pipefail
source "$(dirname "$0")/../lib.sh"
stage=$(mktemp -d)
trap 'rm -rf "$stage"' EXIT
cp -r "${EMBER_PY:-$REPO/python}/ember" "$stage/ember"
cp "$BENCH/parquet_sort.py" "$stage/"
docker build -q -t ember-py:dev -f "$BENCH/kind/Dockerfile.ember-py" "$stage"
kind load docker-image --name ember ember-py:dev
