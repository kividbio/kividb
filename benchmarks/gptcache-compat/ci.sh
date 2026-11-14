#!/usr/bin/env bash
# CI entry point for this harness (the contract is in benchmarks/README.md).
#   ci.sh setup DIR              GPTCache 0.1.44 (tag 0.1.44) on Python 3.11, with requirements.txt
#   ci.sh run DIR PORT OUT.xml   its Redis tests, against 127.0.0.1:PORT
# The tests connect to localhost:6379, so PORT must be 6379. The ONNX test downloads its small
# embedding model from Hugging Face; no API key is needed.
set -euo pipefail
here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
cmd=$1 dir=$2
case $cmd in
  setup)
    rm -rf "$dir/gptcache" "$dir/venv"
    git clone -q --depth 1 --branch 0.1.44 https://github.com/zilliztech/GPTCache "$dir/gptcache"
    # --seed: GPTCache pip-installs what a test imports on the fly, so the venv needs pip.
    uv venv -q --seed --python 3.11 "$dir/venv"
    VIRTUAL_ENV=$dir/venv uv pip install -q -e "$dir/gptcache" -r "$here/requirements.txt"
    ;;
  run)
    port=$3 out=$4
    [ "$port" = 6379 ] || { echo "GPTCache's tests expect port 6379, not $port" >&2; exit 2; }
    cd "$dir/gptcache"
    PATH=$dir/venv/bin:$PATH timeout 900 "$dir/venv/bin/pytest" \
      tests/unit_tests/manager/test_redis.py tests/unit_tests/manager/test_redis_cache_storage.py \
      tests/unit_tests/eviction/test_distributed_cache.py tests/integration_tests/test_redis_onnx.py \
      -p no:cacheprovider -o addopts="" --junitxml="$out" || true
    test -s "$out"
    ;;
  *) echo "usage: ci.sh setup DIR | ci.sh run DIR PORT OUT.xml" >&2; exit 2 ;;
esac
