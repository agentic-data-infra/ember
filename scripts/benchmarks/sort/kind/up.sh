#!/usr/bin/env bash
# (Re)creates the kind cluster "ember" from cluster.yaml with $SORTBENCH_DATA mounted, starts Kiln and the Ember workers
# as deploy/kind/up.sh does (from the image ember:dev, deploy/build_image.sh), and loads the Parquet driver's image
# (build_image.sh).
set -euo pipefail
source "$(dirname "$0")/../lib.sh"
mkdir -p "$DATA/kind-out"
kind delete cluster --name ember
sed -e "s|DATA_DIR|$DATA|" -e "s|OUT_DIR|$DATA/kind-out|" "$BENCH/kind/cluster.yaml" | kind create cluster --config=-
"$REPO/deploy/kind/up.sh"
"$BENCH/kind/build_image.sh"
