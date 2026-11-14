#!/usr/bin/env bash
# Runs bench.py per configuration on a fresh server; see README.md.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
: "${BASELINE:?path to the master kividb binary}"
: "${CURRENT:?path to the new kividb binary}"
REDIS=${REDIS:-redis-server}
PYTHON=${PYTHON:-python3}
OUT=${OUT:-$here/results/run.jsonl}
DIMS=${DIMS:-"128 768 1536"}
SIZES=${SIZES:-"10000 50000"}
PORT=${PORT:-16500}
ONLY=${ONLY:-}
mkdir -p "$(dirname "$OUT")"
work=$(mktemp -d)
trap 'kill $(jobs -p) 2>/dev/null || true; rm -rf "$work"' EXIT

start() { # engine binary
  local engine=$1 bin=$2
  rm -rf "$work"/*
  if [ "$engine" = redis ]; then
    "$bin" --port "$PORT" --save '' --appendonly no --dir "$work" >"$work/log" 2>&1 &
  else
    (cd "$work" && "$bin" --port "$PORT" --bind 127.0.0.1 >"$work/log" 2>&1) &
  fi
  for _ in $(seq 100); do
    redis-cli -p "$PORT" ping >/dev/null 2>&1 && return
    sleep 0.1
  done
  echo "server did not start"; cat "$work/log"; exit 1
}

stop() {
  redis-cli -p "$PORT" shutdown nosave >/dev/null 2>&1 || true
  wait 2>/dev/null || true
}

run() { # label engine binary quant [extra]
  local label=$1 engine=$2 bin=$3 quant=$4; shift 4
  # ONLY is a regex of labels to run.
  if [ -n "$ONLY" ] && ! [[ $label =~ $ONLY ]]; then return; fi
  for dim in $DIMS; do
    for n in $SIZES; do
      start "$engine" "$bin"
      "$PYTHON" "$here/bench.py" --port "$PORT" --label "$label" --quant "$quant" \
        --dim "$dim" --n "$n" --out "$OUT" "$@"
      stop
    done
  done
}

run master-f32 kividb "$BASELINE" ""
run kividb-noquant kividb "$CURRENT" NOQUANT
run kividb-q8 kividb "$CURRENT" Q8
run kividb-bin kividb "$CURRENT" BIN
run redis-f32-truth redis "$REDIS" NOQUANT --truth
run redis-q8-truth redis "$REDIS" Q8 --truth
run redis-bin-truth redis "$REDIS" BIN --truth
run redis-q8-hnsw redis "$REDIS" Q8
