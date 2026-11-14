#!/usr/bin/env bash
# CI entry point for this harness (the contract is in benchmarks/README.md).
#   ci.sh setup DIR              a Python 3.12 venv with requirements.txt (redisvl 0.32.0, redis-py 8.1.0, ...)
#   ci.sh run DIR PORT OUT.xml   redisvl_kividb_compat.py against 127.0.0.1:PORT, as JUnit XML
# The checks are this folder's harness, which drives RedisVL's own SearchIndex, HybridQuery,
# AggregateHybridQuery, SemanticCache and SemanticRouter (REPORT.md). A stub plays the embedding
# model, so nothing is downloaded beyond the pinned packages and no API key is needed. Any port works.
set -euo pipefail
here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
cmd=${1:-} dir=${2:-}
case $cmd in
  setup)
    rm -rf "$dir/venv"
    uv venv -q --python 3.12 "$dir/venv"
    VIRTUAL_ENV=$dir/venv uv pip install -q -r "$here/requirements.txt"
    ;;
  run)
    port=$3 out=$4
    # The harness exits 1 when a check fails: that is a result, written to OUT.xml, not a failed run.
    timeout 300 "$dir/venv/bin/python" "$here/redisvl_kividb_compat.py" \
      --url "redis://127.0.0.1:$port" --label ci --json "$dir/results.json" --junitxml "$out" || true
    test -s "$out"
    ;;
  *) echo "usage: ci.sh setup DIR | ci.sh run DIR PORT OUT.xml" >&2; exit 2 ;;
esac
