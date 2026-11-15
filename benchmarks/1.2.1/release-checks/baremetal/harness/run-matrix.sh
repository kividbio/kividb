#!/usr/bin/env bash
# Run the benchmark matrix against one engine, from the client instance.
#
#   scripts/run-matrix.sh <label> <server-ip> <port> [cluster]
#
# For each value size: empty the server, write every key once, then run each
# scenario for a fixed time against the full keyspace.
#
#   value sizes   100 bytes and 1 KB                     (SIZES)
#   pipelines     1 and 16                               (PIPELINES)
#   ratios        write-only, read-only, 1 write:10 read (RATIOS, as set:get)
#   duration      60 s a scenario, after the load        (DURATION)
#   keys          10 million                             (KEYS)
#
# A scenario is timed, not counted (--test-time, not -n): with a request
# count, a fast engine finishes in seconds and is measured warming up, while
# a slow one runs for minutes. Value size is always given: memtier's default
# is 32 bytes, the case most flattering to any engine.
#
# Results go to ~/results/<label>/: memtier's text and JSON for each
# scenario, the server's memory after each load, and summary.csv.
#
# Environment: SIZES, PIPELINES, RATIOS, DURATION, KEYS, MEMTIER_THREADS
# (default 60), OUT.
set -euo pipefail

LABEL=${1:?usage: run-matrix.sh <label> <server-ip> <port> [cluster]}
SERVER=${2:?server ip}
PORT=${3:?port}
MODE=${4:-}
HERE=$(cd "$(dirname "$0")" && pwd)
OUT=${OUT:-$HOME/results/$LABEL}
SIZES=${SIZES:-"100 1024"}
PIPELINES=${PIPELINES:-"1 16"}
RATIOS=${RATIOS:-"1:0 0:1 1:10"}
DURATION=${DURATION:-60}
KEYS=${KEYS:-10000000}
THREADS=${MEMTIER_THREADS:-60}
mkdir -p "$OUT"

common=(-s "$SERVER" -p "$PORT" --distinct-client-seed --hide-histogram
  --key-minimum=1 "--key-maximum=$KEYS")
[ "$MODE" = cluster ] && common+=(--cluster-mode)

cli() { redis-cli -h "$SERVER" -p "$PORT" "$@"; }

flush() {
  if [ "$MODE" = cluster ]; then
    redis-cli --cluster call "$SERVER:$PORT" flushall > /dev/null
  else
    cli flushall > /dev/null
  fi
}

memory() { # size
  # used_memory and key count after the load: bytes per key, engine by engine.
  if [ "$MODE" = cluster ]; then
    redis-cli --cluster call "$SERVER:$PORT" info memory | grep -a "used_memory:" \
      | awk -F: '{s += $NF} END {print "used_memory:" s}'
    redis-cli --cluster call "$SERVER:$PORT" dbsize | awk '{s += $NF} END {print "keys:" s}'
  else
    # Not every engine reports used_memory (Garnet does not): leave it blank.
    cli info memory | grep -a "^used_memory:" | tr -d '\r' || true
    echo "keys:$(cli dbsize | tr -d '\r')"
  fi > "$OUT/memory-$1.txt"
  cat "$OUT/memory-$1.txt"
}

{
  echo "label=$LABEL server=$SERVER port=$PORT mode=${MODE:-single}"
  echo "sizes=$SIZES pipelines=$PIPELINES ratios=$RATIOS duration=$DURATION keys=$KEYS threads=$THREADS"
  memtier_benchmark --version 2>&1 | head -1
  date -u +%FT%TZ
} | tee "$OUT/run.txt"

for size in $SIZES; do
  flush
  echo "== load: $KEYS keys of $size bytes"
  memtier_benchmark "${common[@]}" --ratio 1:0 --key-pattern P:P -n allkeys \
    -t "$THREADS" -c 5 --pipeline 16 -d "$size" > "$OUT/load-$size.txt" 2>&1
  memory "$size"
  for pipeline in $PIPELINES; do
    # More connections where each carries one request at a time.
    clients=5
    [ "$pipeline" = 1 ] && clients=20
    for ratio in $RATIOS; do
      name="d${size}_p${pipeline}_r${ratio/:/-}"
      echo "== $name ($DURATION s)"
      memtier_benchmark "${common[@]}" --ratio "$ratio" --key-pattern R:R \
        "--test-time=$DURATION" -t "$THREADS" -c "$clients" --pipeline "$pipeline" \
        -d "$size" --json-out-file "$OUT/$name.json" > "$OUT/$name.txt" 2>&1
      grep -a "^Totals" "$OUT/$name.txt" | tail -1
    done
  done
done
python3 "$HERE/summarize.py" "$OUT" > "$OUT/summary.csv"
echo "results in $OUT"
cat "$OUT/summary.csv"
