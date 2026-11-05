#!/usr/bin/env bash
# One leg of linux-verification.yml's io-model-matrix job: real traffic
# through one I/O model. Keep the two in step; the workflow explains why each
# step exists.
#
#   io_model_matrix.sh <kividb-binary> <tokio|uring> [<log-dir>]
#
# Run from the repository root.

set -uo pipefail
BIN=$(readlink -f "$1")
MODEL=$2
LOGS=${3:-/tmp}
export KIVI_BINARY=$BIN
failed=()

wait_up() { # port, log
  for _ in $(seq 1 60); do
    redis-cli -p "$1" ping >/dev/null 2>&1 && return 0
    sleep 1
  done
  echo "server on port $1 never became reachable"
  cat "$2"
  return 1
}

step() { # name, command...
  local name=$1
  shift
  echo "--- $name"
  "$@" || failed+=("$name")
}

# Servers only: "$BIN" alone would also match this script, which has the
# binary's path among its arguments.
stop_all() { pkill -9 -f "^$BIN --port" 2>/dev/null; sleep 1; }
trap stop_all EXIT
stop_all

echo "kernel $(uname -r); io_uring_disabled=$(cat /proc/sys/kernel/io_uring_disabled 2>/dev/null || echo n/a)"

MASTER_LOG=$LOGS/kividb-$MODEL.log
"$BIN" --port 6380 --bind 127.0.0.1 --loglevel notice --io-model "$MODEL" \
  > "$MASTER_LOG" 2>&1 &
wait_up 6380 "$MASTER_LOG" || exit 1
# A silent fallback to tokio would make the uring leg meaningless.
grep -i "I/O model" "$MASTER_LOG"
if [ "$MODEL" = uring ] && grep -q "io_uring unavailable" "$MASTER_LOG"; then
  echo "io_uring was asked for and is not in use"
  exit 1
fi

step "pub/sub matrix" python smoke/pubsub_matrix.py --port 6380 --label "$MODEL"
step "blocking matrix" python smoke/blocking_matrix.py --port 6380 --label "$MODEL"

replication() {
  local log=$LOGS/kividb-replica-$MODEL.log
  # A directory of its own: the master holds this one's kividb.lock.
  local dir
  dir=$(mktemp -d)
  (cd "$dir" && "$BIN" --port 6381 --bind 127.0.0.1 --loglevel notice --io-model "$MODEL" \
    --replicaof 127.0.0.1 6380 > "$log" 2>&1 &)
  wait_up 6381 "$log" || return 1
  python smoke/replication_determinism.py --master-port 6380 --replica-port 6381 --label "$MODEL"
  local status=$?
  redis-cli -p 6381 shutdown nosave >/dev/null 2>&1
  redis-cli -p 6380 flushall >/dev/null 2>&1
  return $status
}
step "replication determinism" replication

aof_no_loss() {
  local dir
  dir=$(mktemp -d)
  (cd "$dir" && "$BIN" --port 6382 --bind 127.0.0.1 --aof --loglevel error \
    --io-model "$MODEL" > "$dir/server.log" 2>&1 &)
  wait_up 6382 "$dir/server.log" || return 1
  python smoke/aof_no_loss.py --port 6382 --dir "$dir" -n 200000 --label "$MODEL"
  local status=$?
  redis-cli -p 6382 shutdown nosave >/dev/null 2>&1
  return $status
}
step "AOF loses no acknowledged write" aof_no_loss

aof_rewrite() {
  local dir
  dir=$(mktemp -d)
  start() {
    (cd "$dir" && "$BIN" --port 6383 --bind 127.0.0.1 --aof --loglevel error \
      --io-model "$MODEL" >> "$dir/server.log" 2>&1 &)
    wait_up 6383 "$dir/server.log"
  }
  start || return 1
  python smoke/aof_rewrite_durability.py --port 6383 --dir "$dir" || { cat "$dir/server.log"; return 1; }
  # Hard kill: a graceful stop would write dump.kdb and test nothing.
  pkill -9 -f "^$BIN --port 6383"
  sleep 2
  start || return 1
  python smoke/aof_rewrite_durability.py --port 6383 --dir "$dir" --verify-only
  local status=$?
  redis-cli -p 6383 shutdown nosave >/dev/null 2>&1
  return $status
}
step "AOF rewrite loses nothing" aof_rewrite

step "replica AOF durability" python smoke/replica_aof_durability.py \
  --binary "$BIN" --io-model "$MODEL" --master-port 6384 --replica-port 6385 -n 5000

e2e() { # resp
  (cd tests/e2e && KIVI_EXTERNAL=1 KIVI_PORT=6380 KIVI_RESP=$1 pytest modules/ -q -p no:cacheprovider)
}
step "e2e RESP2" e2e 2
step "e2e RESP3" e2e 3

if [ ${#failed[@]} -gt 0 ]; then
  echo "FAILED ($MODEL): ${failed[*]}"
  echo "--- server log tail"
  tail -100 "$MASTER_LOG"
  exit 1
fi
echo "all passed ($MODEL)"
