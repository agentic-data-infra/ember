#!/usr/bin/env bash
# Builds Axiom (and with it Velox) with CMake, then regenerates the Buck
# wrapper flags in third-party/generated/deps.bzl.
#
# Ember uses a single Velox checkout: Axiom's velox/ submodule directory is
# replaced by a symlink to $VELOX_SRC. Velox's Spark SQL functions are enabled
# so Ember can give Spark semantics.
set -euo pipefail

EMBER_DIR="$(cd "$(dirname "$0")/.." && pwd)"
AXIOM_SRC="${AXIOM_SRC:-$HOME/source/axiom}"
VELOX_SRC="${VELOX_SRC:-$HOME/source/velox}"
BUILD_DIR="${BUILD_DIR:-$AXIOM_SRC/_build/release}"
NUM_THREADS="${NUM_THREADS:-$(nproc)}"

if [ ! -L "$AXIOM_SRC/velox" ]; then
  rmdir "$AXIOM_SRC/velox"  # Fails (safely) if the submodule is populated.
  ln -s "$VELOX_SRC" "$AXIOM_SRC/velox"
fi

cmake -S "$AXIOM_SRC" -B "$BUILD_DIR" -GNinja \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_PROJECT_INCLUDE="$EMBER_DIR/scripts/axiom_preload.cmake" \
  -DAXIOM_BUILD_TESTING=ON \
  -DAXIOM_BUILD_TEST_UTILS=ON \
  -DVELOX_ENABLE_SPARK_FUNCTIONS=ON \
  -DVELOX_MONO_LIBRARY=ON \
  -DVELOX_BUILD_SHARED=OFF \
  -DCMAKE_C_COMPILER_LAUNCHER=ccache \
  -DCMAKE_CXX_COMPILER_LAUNCHER=ccache \
  -DCMAKE_POLICY_VERSION_MINIMUM=3.5

ninja -C "$BUILD_DIR" -j "$NUM_THREADS"

"$EMBER_DIR/scripts/gen_third_party.py" --axiom-src "$AXIOM_SRC" --axiom-build "$BUILD_DIR"
