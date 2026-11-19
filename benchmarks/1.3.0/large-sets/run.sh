#!/usr/bin/env bash
# Runs bench.py's tests against each engine in turn, on one machine, each
# server fresh for each test, and writes results/<label>/results.jsonl.
#
#   run.sh <label> name=path [name=path ...]      e.g.
#   run.sh d4s_v5 master=~/bins/master/kividb branch=target/release/kividb \
#       redis=/opt/redis-8.10.2/src/redis-server
#
# Env: TESTS="fill sscan rand copy free background digest" (the default),
# PORT (6500), PYTHON (python3 with redis-py), REDIS_BENCHMARK.
set -u
HERE=$(cd "$(dirname "$0")" && pwd)
LABEL=${1:?usage: run.sh <label> name=path...}; shift
OUT="$HERE/results/$LABEL"; mkdir -p "$OUT"
PORT=${PORT:-6500} PY=${PYTHON:-python3}
for test in ${TESTS:-fill sscan rand copy free background digest}; do
  for spec in "$@"; do
    name=${spec%%=*} bin=${spec#*=}
    dir=$(mktemp -d)
    case $(basename "$bin") in
      redis-server*) (cd "$dir" && exec "$bin" --port "$PORT" --save "" --appendonly no \
                        --enable-debug-command yes > server.log 2>&1) & ;;
      *) (cd "$dir" && exec "$bin" --port "$PORT" --bind 127.0.0.1 --enable-debug-command yes \
                        > server.log 2>&1) & ;;
    esac
    for _ in $(seq 100); do redis-cli -p "$PORT" ping 2>/dev/null | grep -q PONG && break; sleep 0.1; done
    pid=$(pgrep -f -n -- "--port $PORT")
    "$PY" "$HERE/bench.py" "$test" --port "$PORT" --pid "$pid" --label "$name" --out "$OUT/results.jsonl" \
      || echo "{\"test\": \"$test\", \"label\": \"$name\", \"failed\": true}" >> "$OUT/results.jsonl"
    redis-cli -p "$PORT" shutdown nosave > /dev/null 2>&1
    sleep 1; kill -9 "$pid" 2>/dev/null; wait 2>/dev/null
    rm -rf "$dir"
  done
done
echo "results in $OUT/results.jsonl"
