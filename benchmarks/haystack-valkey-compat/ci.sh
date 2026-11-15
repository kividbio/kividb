#!/usr/bin/env bash
# CI entry point for this harness (the contract is in benchmarks/README.md).
#   ci.sh setup DIR              valkey-haystack 1.0.0 (tag integrations/valkey-v1.0.0), with requirements.txt
#   ci.sh run DIR PORT OUT.xml   its integration tests (pytest -m integration), against 127.0.0.1:PORT
# The tests connect to localhost:6379, so PORT must be 6379.
set -euo pipefail
here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
cmd=$1 dir=$2
case $cmd in
  setup)
    rm -rf "$dir/hci" "$dir/venv"
    git clone -q --depth 1 --branch integrations/valkey-v1.0.0 --filter=blob:none --sparse \
      https://github.com/deepset-ai/haystack-core-integrations "$dir/hci"
    git -C "$dir/hci" sparse-checkout set integrations/valkey
    uv venv -q --python 3.12 "$dir/venv"
    VIRTUAL_ENV=$dir/venv uv pip install -q -e "$dir/hci/integrations/valkey" -r "$here/requirements.txt"
    ;;
  run)
    port=$3 out=$4
    [ "$port" = 6379 ] || { echo "haystack-valkey's tests expect port 6379, not $port" >&2; exit 2; }
    cd "$dir/hci/integrations/valkey"
    timeout 600 "$dir/venv/bin/pytest" -m integration tests -p no:cacheprovider --junitxml="$out" || true
    test -s "$out"
    ;;
  *) echo "usage: ci.sh setup DIR | ci.sh run DIR PORT OUT.xml" >&2; exit 2 ;;
esac
