#!/usr/bin/env bash
# CI entry point for this harness (the contract is in benchmarks/README.md).
#   ci.sh setup DIR              semantic-kernel 1.47.0 from its python-1.47.0 tag, with its own uv.lock
#   ci.sh run DIR PORT OUT.xml   its Redis cases of the vector store integration test, against 127.0.0.1:PORT
set -euo pipefail
cmd=$1 dir=$2
case $cmd in
  setup)
    rm -rf "$dir/sk"
    git clone -q --depth 1 --branch python-1.47.0 --filter=blob:none --sparse \
      https://github.com/microsoft/semantic-kernel "$dir/sk"
    git -C "$dir/sk" sparse-checkout set python
    cd "$dir/sk/python"
    uv sync -q --frozen --python 3.12 --extra redis --group dev
    uv pip install -q pandas==3.1.1
    ;;
  run)
    port=$3 out=$4
    cd "$dir/sk/python"
    REDIS_CONNECTION_STRING=redis://127.0.0.1:$port timeout 600 \
      uv run --no-sync pytest tests/integration/memory/test_vector_store.py -k redis \
      -p no:cacheprovider --junitxml="$out" || true
    test -s "$out"
    ;;
  *) echo "usage: ci.sh setup DIR | ci.sh run DIR PORT OUT.xml" >&2; exit 2 ;;
esac
