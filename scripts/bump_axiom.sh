#!/usr/bin/env bash
# Rebases Ember's Axiom fork onto upstream Axiom and builds and tests Ember with
# it. See "Updating Axiom and Velox" in README.md.
#
# Creates branch ember-YYYYMMDD (today) in $AXIOM_SRC from upstream main,
# cherry-picks onto it the fork commits of the newest earlier ember-YYYYMMDD
# branch, moves $VELOX_SRC to the Velox commit Axiom pins, points AXIOM_REF in
# scripts/build_deps.sh at the branch, rebuilds, and runs the Axiom optimizer
# and file connector tests and Ember's tests. It never pushes or tags: it prints
# the commands that do. If today's branch exists it is reused, so after fixing a
# conflict or a failure, run the script again.
set -euo pipefail

EMBER_DIR="$(cd "$(dirname "$0")/.." && pwd)"
export AXIOM_SRC="${AXIOM_SRC:-$HOME/source/axiom-next}"
export VELOX_SRC="${VELOX_SRC:-$HOME/source/velox-next}"
export BUILD_DIR="${BUILD_DIR:-$AXIOM_SRC/_build/release-gcc12}"

UPSTREAM_REPO=https://github.com/facebookincubator/axiom.git
FORK_REPO=https://github.com/agentic-data-infra/axiom.git
VELOX_REPO=https://github.com/facebookincubator/velox.git
BRANCH="ember-$(date +%Y%m%d)"

axiom() { git -C "$AXIOM_SRC" "$@"; }

if axiom rev-parse -q --verify CHERRY_PICK_HEAD >/dev/null; then
  echo "A cherry-pick is in progress in $AXIOM_SRC: finish it first." >&2
  exit 1
fi

if axiom rev-parse -q --verify "refs/heads/$BRANCH" >/dev/null; then
  echo "Reusing branch $BRANCH."
  axiom checkout -q "$BRANCH"
else
  axiom fetch -q "$UPSTREAM_REPO" main
  upstream="$(axiom rev-parse FETCH_HEAD)"
  prev="$(git ls-remote --heads "$FORK_REPO" |
    sed -n 's|.*refs/heads/\(ember-[0-9]\{8\}\)$|\1|p' |
    awk -v today="$BRANCH" '$0 < today' | sort | tail -n 1)"
  if [ -z "$prev" ]; then
    echo "No earlier ember-YYYYMMDD branch in $FORK_REPO." >&2
    exit 1
  fi
  axiom fetch -q "$FORK_REPO" "refs/heads/$prev"
  fork="$(axiom rev-parse FETCH_HEAD)"
  echo "Creating $BRANCH from upstream main ($(axiom rev-parse --short "$upstream")) with the fork commits of $prev:"
  axiom log --reverse --format='  %h %s' "$upstream..$fork"
  axiom checkout -q -b "$BRANCH" "$upstream"
  if ! axiom cherry-pick "$upstream..$fork"; then
    cat >&2 <<EOF

Cherry-pick conflict in $AXIOM_SRC (see above). Resolve it and run
'git cherry-pick --continue' (or '--skip' if upstream already has the change),
until all commits are picked. Then run $0 again.
EOF
    exit 1
  fi
fi

velox_ref="$(axiom rev-parse HEAD:velox)"
if ! git -C "$VELOX_SRC" rev-parse -q --verify "$velox_ref^{commit}" >/dev/null; then
  git -C "$VELOX_SRC" fetch -q "$VELOX_REPO" "$velox_ref"
fi
git -C "$VELOX_SRC" checkout -q --detach "$velox_ref"

sed -i "s/^AXIOM_REF=.*/AXIOM_REF=$BRANCH/" "$EMBER_DIR/scripts/build_deps.sh"
"$EMBER_DIR/scripts/build_deps.sh"

# Axiom's tests read their data relative to the source root.
(
  cd "$AXIOM_SRC"
  for test in optimizer/tests/axiom_optimizer_tests \
              connectors/file/tests/axiom_file_connector_test \
              connectors/file/tests/axiom_file_handler_test; do
    "$BUILD_DIR/axiom/$test"
  done
)

# Buck does not see that the Axiom and Velox libraries changed.
(cd "$EMBER_DIR" && buck2 clean && buck2 test //...)

cat <<EOF

All tests pass. To publish the fork version (tag suffix -1, or the next free
number):

  git -C $AXIOM_SRC tag -a $BRANCH-1 -m "Ember's Axiom fork, $BRANCH"
  git -C $AXIOM_SRC push git@github.com:agentic-data-infra/axiom.git $BRANCH $BRANCH-1
  sed -i 's/^AXIOM_REF=.*/AXIOM_REF=$BRANCH-1/' $EMBER_DIR/scripts/build_deps.sh

Then commit Ember's changes and open a pull request.
EOF
