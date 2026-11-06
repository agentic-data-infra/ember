#!/usr/bin/env bash
# Builds Axiom (and with it Velox) with CMake and GCC 12 against the deps in
# $PREFIX (Velox's setup scripts, built with GCC 12), then regenerates the
# Buck wrapper flags in third-party/generated/deps.bzl.
#
# Ember uses a single Velox checkout: Axiom's velox/ submodule directory is
# replaced by a symlink to $VELOX_SRC. Velox's sparksql functions are enabled
# for Ember's function semantics.
set -euo pipefail

EMBER_DIR="$(cd "$(dirname "$0")/.." && pwd)"
AXIOM_SRC="${AXIOM_SRC:-$HOME/source/axiom}"
VELOX_SRC="${VELOX_SRC:-$HOME/source/velox}"
PREFIX="${PREFIX:-/opt/deps-gcc12}"
BUILD_DIR="${BUILD_DIR:-$AXIOM_SRC/_build/release-gcc12}"
NUM_THREADS="${NUM_THREADS:-$(nproc)}"

# The Axiom version Ember builds with, set only here; it pins Velox's. See
# "Updating Axiom and Velox" in README.md.
AXIOM_REPO=https://github.com/agentic-data-infra/axiom.git
AXIOM_REF=ember-20261105-3
VELOX_REPO=https://github.com/facebookincubator/velox.git

# Clones 'repo' at 'ref' into 'dir' if it is missing, and fails if 'dir' sits
# at another commit: it may hold work, so it is not moved.
check_source() {
  local repo=$1 ref=$2 dir=$3
  if [ ! -d "$dir" ]; then
    git clone "$repo" "$dir"
    git -C "$dir" checkout --detach "$ref"
  fi
  git -C "$dir" rev-parse -q --verify "$ref^{commit}" >/dev/null ||
    git -C "$dir" fetch "$repo" --tags
  if [ "$(git -C "$dir" rev-parse HEAD)" != \
       "$(git -C "$dir" rev-parse "$ref^{commit}")" ]; then
    echo "$dir is not at $ref of $repo" >&2
    exit 1
  fi
}

check_source "$AXIOM_REPO" "$AXIOM_REF" "$AXIOM_SRC"
# The Velox commit that Axiom's submodule pins.
VELOX_REF="$(git -C "$AXIOM_SRC" rev-parse "$AXIOM_REF:velox")"
check_source "$VELOX_REPO" "$VELOX_REF" "$VELOX_SRC"

if [ ! -L "$AXIOM_SRC/velox" ]; then
  rmdir "$AXIOM_SRC/velox"  # Fails (safely) if the submodule is populated.
  ln -s "$VELOX_SRC" "$AXIOM_SRC/velox"
fi

export CC=gcc-12 CXX=g++-12 PATH="$PREFIX/bin:$PATH"
cmake -S "$AXIOM_SRC" -B "$BUILD_DIR" -GNinja \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_PREFIX_PATH="$PREFIX" \
  -DCMAKE_IGNORE_PREFIX_PATH=/usr/local \
  -DCMAKE_PROJECT_INCLUDE="$EMBER_DIR/scripts/axiom_preload.cmake" \
  -DAXIOM_BUILD_TESTING=ON \
  -DAXIOM_BUILD_TEST_UTILS=ON \
  -DVELOX_ENABLE_SPARK_FUNCTIONS=ON \
  -DVELOX_MONO_LIBRARY=ON \
  -DVELOX_BUILD_SHARED=OFF \
  -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
  -DCMAKE_C_COMPILER_LAUNCHER=ccache \
  -DCMAKE_CXX_COMPILER_LAUNCHER=ccache \
  -DCMAKE_POLICY_VERSION_MINIMUM=3.5

ninja -C "$BUILD_DIR" -j "$NUM_THREADS"

"$EMBER_DIR/scripts/gen_third_party.py" --prefix "$PREFIX" --axiom-src "$AXIOM_SRC" --axiom-build "$BUILD_DIR"
