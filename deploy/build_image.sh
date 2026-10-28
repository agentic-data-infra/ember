#!/usr/bin/env bash
# Builds Ember's container image (TCP only, no RDMA).
#
#   deploy/build_image.sh [tag]      # default ember:dev
set -euo pipefail

tag=${1:-ember:dev}
root=$(cd "$(dirname "$0")/.." && pwd)
stage=$(mktemp -d)
trap 'rm -rf "$stage"' EXIT
mkdir -p "$stage/bin" "$stage/lib" "$stage/gensort"

cd "$root"
buck2 build //bin:ember_worker //examples:sort_benchmark \
  kiln//bin:kiln_meta kiln//bin:kiln_server --show-full-output |
  while read -r _ path; do cp "$path" "$stage/bin/"; done
cp scripts/gensort.sh "$stage/bin/"

# The libraries the binaries link, except glibc's, which the base image has.
for bin in "$stage"/bin/*; do
  [[ $bin == *.sh ]] || ldd "$bin" | awk '$3 ~ /^\// {print $3}'
done | sort -u | grep -vE '/(libc|libm|libdl|libpthread|librt|ld-linux[^/]*)\.so' |
  xargs -I{} cp -L {} "$stage/lib/"

tools=$(scripts/gensort.sh install)
cp $tools "$stage/gensort/"

docker build -t "$tag" -f deploy/Dockerfile "$stage"
