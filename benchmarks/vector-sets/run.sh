#!/usr/bin/env bash
# Run vset_bench.py against KiviDB and Redis, one server at a time, on this box.
#
#   KIVIDB=~/kividb/target/release/kividb REDIS=/opt/redis-8.10.2/src/redis-server \
#     PY=~/venv/bin/python SIZES="10000 100000 1000000" REDIS_NOQUANT="10000 100000" \
#     OUT=results bash run.sh
#
# Each server starts empty with persistence off, gets every size in turn, and
# is shut down before the next server starts.
set -eu
HERE=$(cd "$(dirname "$0")" && pwd)
KIVIDB=${KIVIDB:-$HOME/kividb/target/release/kividb}
REDIS=${REDIS:-/opt/redis-8.10.2/src/redis-server}
PY=${PY:-python3}
SIZES=${SIZES:-10000 100000}
DIM=${DIM:-128}
EF=${EF:-default,200,800}
CLIENTS=${CLIENTS:-1,16,100}
DURATION=${DURATION:-10}
OUT=${OUT:-$HERE/results}
PORT=${PORT:-6399}
mkdir -p "$OUT"

wait_up() { for _ in $(seq 1 50); do "$PY" -c "import redis;redis.Redis(port=$PORT).ping()" 2>/dev/null && return 0; sleep 0.2; done; echo "server did not start" >&2; exit 1; }
stop() { "$PY" -c "import redis
try: redis.Redis(port=$PORT).shutdown(nosave=True)
except Exception: pass" ; sleep 1; }

bench() {  # label size [vadd-opts]
  local ef=$EF
  # KIVIDB_EF=default: KiviDB's VSIM is exact and ignores EF, so one sweep point is enough
  if [ "$1" = kividb ]; then ef=${KIVIDB_EF:-$EF}; fi
  "$PY" "$HERE/vset_bench.py" --port "$PORT" --label "$1" --n "$2" --dim "$DIM" \
    --ef "$ef" --clients "$CLIENTS" --duration "$DURATION" --vadd-opts "${3:-}" \
    --out "$OUT/$1-n$2-d$DIM.json"
}

echo "== kividb $("$KIVIDB" --version 2>/dev/null | head -1)"
(cd "$OUT" && exec "$KIVIDB" --port "$PORT" --bind 127.0.0.1 --loglevel warning) >"$OUT/kividb.log" 2>&1 &
wait_up
for n in $SIZES; do bench kividb "$n"; done
stop

echo "== redis $("$REDIS" --version)"
"$REDIS" --port "$PORT" --bind 127.0.0.1 --save "" --appendonly no --loglevel warning >"$OUT/redis.log" 2>&1 &
wait_up
for n in $SIZES; do
  bench redis "$n"                  # Redis's default: Q8
  # REDIS_NOQUANT="10000 100000": also run these sizes with NOQUANT (f32, like KiviDB)
  case " ${REDIS_NOQUANT:-} " in *" $n "*) bench redis-noquant "$n" NOQUANT ;; esac
done
stop
