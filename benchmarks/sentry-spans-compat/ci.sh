#!/usr/bin/env bash
# CI entry point for this harness (the contract is in benchmarks/README.md).
#   ci.sh setup DIR              fetch Sentry's add-buffer.lua at the pinned commit into DIR (SHA-256 checked)
#   ci.sh run DIR PORT OUT.xml   sentry_spans_compat.py against 127.0.0.1:PORT, as JUnit XML
# getsentry/sentry's span buffer script, run as Sentry runs it. It needs scripting, so the
# KiviDB side runs the `lua` image variant (kividb-variant). Any port works.
set -euo pipefail
here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SENTRY_COMMIT=7a8c7b6b236d0caf353a6d4db6882284e50fc5ba
SCRIPT_SHA256=3e5494e7274172f3882288a383f2375e3416c5ed115f6537c71a6535e3252055
cmd=${1:-} dir=${2:-}
case $cmd in
  setup)
    mkdir -p "$dir"
    curl -fsSL --retry 3 -o "$dir/add-buffer.lua" \
      "https://raw.githubusercontent.com/getsentry/sentry/$SENTRY_COMMIT/src/sentry/scripts/spans/add-buffer.lua"
    echo "$SCRIPT_SHA256  $dir/add-buffer.lua" | sha256sum -c --quiet -
    ;;
  run)
    port=$3 out=$4
    timeout 300 python3 "$here/sentry_spans_compat.py" --script "$dir/add-buffer.lua" --port "$port" \
      --junitxml "$out" || true
    test -s "$out"
    ;;
  *) echo "usage: ci.sh setup DIR | ci.sh run DIR PORT OUT.xml" >&2; exit 2 ;;
esac
