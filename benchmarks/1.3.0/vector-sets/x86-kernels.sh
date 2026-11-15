#!/usr/bin/env bash
# #793: f32, Q8 and BIN vector sets on x86-64, before and after the run-time
# kernels (AVX-512BW / AVX2 int8 dot product, POPCNT Hamming distance, AVX2
# quantizing). The shape of suite 09 of benchmarks/1.3.0/regressions: 10,000
# vectors of 768 dimensions, bench.py's VADD and VSIM ... COUNT 10 from 8
# clients for 8 s. Each engine and quantization runs REPEATS times on a fresh
# server; the table prints the median. See README.md, "x86-64".
#
#   V122=/path/kividb-1.2.2 BEFORE=/path/kividb-master AFTER=target/release/kividb \
#     REDIS=/opt/redis-8.10.2/src/redis-server PYTHON=python3 ./x86-kernels.sh
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
: "${BEFORE:?path to the kividb binary before the change}"
: "${AFTER:?path to the kividb binary after the change}"
V122=${V122:-}
REDIS=${REDIS:-}
PYTHON=${PYTHON:-python3}
OUT=${OUT:-$here/results/x86-kernels.jsonl}
REPEATS=${REPEATS:-3}
N=${N:-10000}
DIM=${DIM:-768}
PORT=${PORT:-16501}
mkdir -p "$(dirname "$OUT")"
: >"$OUT"
work=$(mktemp -d)
trap 'kill $(jobs -p) 2>/dev/null || true; rm -rf "$work"' EXIT

ping_ok() { "$PYTHON" -c "import redis,sys; redis.Redis(port=$PORT).ping()" 2>/dev/null; }

one() { # label engine binary quant
  local label=$1 engine=$2 bin=$3 quant=$4
  rm -rf "${work:?}"/*
  if [ "$engine" = redis ]; then
    "$bin" --port "$PORT" --save '' --appendonly no --dir "$work" >"$work/log" 2>&1 &
  else
    "$bin" --port "$PORT" --bind 127.0.0.1 --protected-mode no --dir "$work" >"$work/log" 2>&1 &
  fi
  local pid=$!
  for _ in $(seq 100); do ping_ok && break; sleep 0.1; done
  "$PYTHON" "$here/bench.py" --port "$PORT" --label "$label" --quant "$quant" \
    --dim "$DIM" --n "$N" --out "$OUT"
  kill "$pid"
  wait "$pid" 2>/dev/null || true
}

for _ in $(seq "$REPEATS"); do
  # 1.2 has no quantization: stored f32, as suite 09's v1.2.2 column.
  if [ -n "$V122" ]; then one "v1.2.2-f32" kividb "$V122" ""; fi
  for q in NOQUANT Q8 BIN; do
    one "before-$q" kividb "$BEFORE" "$q"
    one "after-$q" kividb "$AFTER" "$q"
    if [ -n "$REDIS" ]; then one "redis-$q" redis "$REDIS" "$q"; fi
  done
done

"$PYTHON" - "$OUT" <<'EOF'
import json, statistics, sys
rows = {}
for line in open(sys.argv[1]):
    r = json.loads(line)
    rows.setdefault(r["label"], []).append(r)
print("| config | VSIM q/s (median) | VSIM p50 ms | VADD/s (median) | recall@10 | runs |")
print("|---|---:|---:|---:|---:|---:|")
for label in sorted(rows):
    rs = rows[label]
    med = lambda k: statistics.median(x[k] for x in rs)
    print(f"| {label} | {med('vsim_qps'):,.0f} | {med('vsim_p50_ms'):.2f} | {med('vadd_per_s'):,.0f} "
          f"| {rs[0]['recall_at_10']:.3f} | {len(rs)} |")
EOF
