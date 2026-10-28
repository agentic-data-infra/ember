#!/usr/bin/env bash
# gensort and valsort from sortbenchmark.org: input for the sort benchmark and
# validation of its output. The tools (GPL 2.0) are downloaded, not part of
# Ember.
#
#   scripts/gensort.sh install
#       Downloads gensort and valsort into $GENSORT_DIR
#       (default ~/.cache/ember/gensort).
#   scripts/gensort.sh generate <records> <dir> [files] [first]
#       Writes <records> binary records into <dir>/part-00000 ... as
#       <files> (default 1) files, together the same records as one gensort
#       run would write, starting at record [first] (default 0).
#   scripts/gensort.sh validate <output> [input]
#       Checks that <output> (a file, or a directory whose files are read in
#       name order) holds sorted records. With [input], also checks that it
#       holds the same records: same count and checksum.
set -euo pipefail

version=1.5
sha256=a0701ab819691fdfe79ffd727390d421284641aa4be9d4e0d74ce8ee9d8dd9fd
dir=${GENSORT_DIR:-$HOME/.cache/ember/gensort}
gensort=$dir/64/gensort
valsort=$dir/64/valsort

usage() {
  sed -n '6,16p' "$0" >&2
  exit 2
}

install() {
  [[ -x "$gensort" && -x "$valsort" ]] && return
  mkdir -p "$dir"
  local tarball=$dir/gensort-linux-$version.tar.gz
  curl -sSfL -o "$tarball" \
    "https://www.ordinal.com/try.cgi/gensort-linux-$version.tar.gz"
  echo "$sha256  $tarball" | sha256sum -c --quiet
  tar xzf "$tarball" -C "$dir"
}

# The files of $1 in name order: the file itself, or a directory's files
# (names starting with "_" or "." skipped, as Ember does).
files() {
  if [[ -d "$1" ]]; then
    find "$1" -maxdepth 1 -type f ! -name '_*' ! -name '.*' | sort
  else
    echo "$1"
  fi
}

# Prints "<records> <checksum>" of the files of $1, read in name order.
# Exits non-zero unless they are sorted.
summary() {
  local tmp
  tmp=$(mktemp -d)
  local i=0 file
  for file in $(files "$1"); do
    "$valsort" -o "$tmp/$(printf '%05d' $i).sum" "$file" >/dev/null 2>&1 || true
    i=$((i + 1))
  done
  [[ $i -gt 0 ]] || { echo "no files in $1" >&2; rm -rf "$tmp"; return 1; }
  cat "$tmp"/*.sum > "$tmp/all.sum"
  local status=0 report
  report=$("$valsort" -s "$tmp/all.sum" 2>&1) || status=$?
  rm -rf "$tmp"
  echo "$(awk '/^Records:/ {print $2}' <<<"$report") $(awk '/^Checksum:/ {print $2}' <<<"$report")"
  if [[ $status -ne 0 ]]; then
    echo "$report" >&2
  fi
  return $status
}

cmd=${1:-}
[[ $# -gt 0 ]] && shift
case $cmd in
  install)
    install
    echo "$gensort"
    echo "$valsort"
    ;;
  generate)
    [[ $# -ge 2 ]] || usage
    install
    records=$1 out=$2 files=${3:-1} start=${4:-0}
    mkdir -p "$out"
    for ((f = 0; f < files; f++)); do
      # The first records % files files get one record more.
      n=$((records / files + (f < records % files ? 1 : 0)))
      "$gensort" -b$start $n "$out/$(printf 'part-%05d' $f)"
      start=$((start + n))
    done
    ;;
  validate)
    [[ $# -ge 1 ]] || usage
    install
    output=$(summary "$1") || { echo "FAIL: $1 is not sorted" >&2; exit 1; }
    echo "output: records checksum = $output"
    if [[ $# -ge 2 ]]; then
      # The input need not be sorted: only its count and checksum matter.
      input=$(summary "$2" 2>/dev/null || true)
      echo "input:  records checksum = $input"
      [[ "$input" == "$output" ]] || { echo "FAIL: records differ" >&2; exit 1; }
    fi
    echo "SUCCESS"
    ;;
  *)
    usage
    ;;
esac
