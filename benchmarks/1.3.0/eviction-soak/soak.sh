#!/usr/bin/env bash
# Writes refused with OOM while eviction works (#520): a master at its
# maxmemory under an allkeys policy, written to by many clients at once,
# with AOF on and a replica that falls behind and is full-synced again.
# Under allkeys-*, as in Redis, a write is to be refused only when nothing
# is left to evict: errorstat_OOM is to stay 0.
#
#   soak.sh <kividb binary> [label]
#
# Environment:
#   DURATION=600        seconds of load after the limit is set
#   MAXMEMORY=512mb     set at runtime, once the dataset is past it
#   POLICY=allkeys-lfu  or allkeys-lru
#   REPLICA=1           a replica with AOF on (0: none)
#   PORT=7400           master; the replica is PORT+1
#   OUT=./soak-<label>  logs, samples and the result line
#
# Needs memtier_benchmark, redis-cli and redis-benchmark on the PATH. Prints
# and writes to $OUT/result.txt one line:
#   <label> oom=<n> fruitless_logs=<n> evicted=<n> writes=<n> oom_per_million=<x>
#           peak_over=<bytes> peak_over_held=<bytes>
# peak_over: the most that used_memory less mem_not_counted_for_evict (what
# maxmemory is measured against) was over maxmemory, sampled every 100 ms
# from 30 s after the limit is set. peak_over_held: the same, of the lower
# of each two samples in a row, so over the limit for 100 ms at least. A
# sample alone can catch the two counts apart (a snapshot's buffers freed,
# and only then taken back from what is not counted): a spike that is gone
# 100 ms later is no write let in, which eviction would take seconds to
# take back out.
set -u
BIN=${1:?usage: soak.sh <kividb binary> [label]}
LABEL=${2:-$(basename "$BIN")}
DURATION=${DURATION:-600} MAXMEMORY=${MAXMEMORY:-512mb} POLICY=${POLICY:-allkeys-lfu}
REPLICA=${REPLICA:-1} PORT=${PORT:-7400}
OUT=${OUT:-$PWD/soak-$LABEL}
RPORT=$((PORT + 1))
rm -rf "$OUT" && mkdir -p "$OUT/master" "$OUT/replica"
pids=()
cleanup() {
  for p in "${pids[@]}"; do kill "$p" 2>/dev/null; done
  redis-cli -p "$RPORT" shutdown nosave >/dev/null 2>&1
  redis-cli -p "$PORT" shutdown nosave >/dev/null 2>&1
  wait 2>/dev/null
}
trap cleanup EXIT

start() { # dir port extra...
  local dir=$1 port=$2; shift 2
  (cd "$dir" && exec "$BIN" --port "$port" --bind 127.0.0.1 --protected-mode no \
    --aof --appendfsync everysec "$@" > server.log 2>&1) &
  pids+=($!)
  for _ in $(seq 100); do redis-cli -p "$port" ping 2>/dev/null | grep -q PONG && return; sleep 0.2; done
  echo "server on $port did not start" >&2; tail -5 "$dir/server.log" >&2; exit 1
}
info() { redis-cli -p "$PORT" info "$1" | tr -d '\r' | awk -F: -v f="$2" '$1 == f { print $2 }'; }

start "$OUT/master" "$PORT" --maxmemory-policy "$POLICY"
[ "$REPLICA" = 1 ] && start "$OUT/replica" "$RPORT" --replicaof 127.0.0.1 "$PORT"

END=$((SECONDS + DURATION + 60))
# Strings, pipelined: SET/GET 1:2, 100-1000 B over 12M keys.
memtier_benchmark -p "$PORT" -t 2 -c 25 --pipeline=16 --ratio=1:2 --data-size-range=100-1000 \
  --key-maximum=12000000 --key-pattern=R:R --random-data --distinct-client-seed \
  --test-time=$((DURATION + 60)) --hide-histogram > "$OUT/memtier-pipelined.log" 2>&1 &
pids+=($!)
# And not pipelined.
memtier_benchmark -p "$PORT" -t 1 -c 50 --ratio=1:1 --data-size-range=100-1000 \
  --key-maximum=12000000 --key-pattern=R:R --random-data --key-prefix=np: \
  --test-time=$((DURATION + 60)) --hide-histogram > "$OUT/memtier-plain.log" 2>&1 &
pids+=($!)
# Hash fields with a time to live.
memtier_benchmark -p "$PORT" -t 1 -c 20 --pipeline=4 --key-maximum=2000000 --key-pattern=R:R \
  --command="HSET __key__ f1 __data__ f2 __data__" --command-ratio=2 \
  --command="HEXPIRE __key__ 20 FIELDS 1 f1" --command-ratio=1 \
  --data-size=200 --key-prefix=hft: --test-time=$((DURATION + 60)) --hide-histogram \
  > "$OUT/memtier-hfe.log" 2>&1 &
pids+=($!)
# Other types.
(while [ $SECONDS -lt $END ]; do
  redis-benchmark -p "$PORT" -q -c 20 -P 8 -r 2000000 -n 200000 -d 300 -t lpush,sadd,hset,zadd,rpush \
    >/dev/null 2>&1
done) &
pids+=($!)

# The dataset grows past the limit, then the limit is set.
sleep 60
redis-cli -p "$PORT" config set maxmemory "$MAXMEMORY" >/dev/null
redis-cli -p "$PORT" config resetstat >/dev/null
echo "t used_memory evicted_keys oom keys" > "$OUT/samples.txt"
t0=$SECONDS
LIMIT=$(redis-cli -p "$PORT" config get maxmemory | tail -1)
# Overshoot over the limit, sampled every 100 ms from 30 s on (before, the
# dataset is still being evicted down to the new limit): the highest goes
# to peak_over.txt.
(peak=0 held=0 last=0
sleep 30
while [ $((SECONDS - t0)) -lt "$DURATION" ]; do
  over=$(redis-cli -p "$PORT" info memory | tr -d '\r' | awk -F: -v l="$LIMIT" \
    '$1 == "used_memory" { u = $2 } $1 == "mem_not_counted_for_evict" { n = $2 }
     END { print u - n - l }')
  over=${over:-0}
  [ "$over" -gt "$peak" ] && peak=$over && echo "$peak" > "$OUT/peak_over.txt"
  low=$(( over < last ? over : last ))
  [ "$low" -gt "$held" ] && held=$low && echo "$held" > "$OUT/peak_over_held.txt"
  last=$over
  # Each sample more than 4 MB over, with the time, to set beside the log.
  [ "$over" -gt 4194304 ] && echo "$(date -u +%H:%M:%S.%N | cut -c1-12) $over" >> "$OUT/over_4mb.txt"
  sleep 0.1
done) &
pids+=($!)
while [ $((SECONDS - t0)) -lt "$DURATION" ]; do
  sleep 10
  echo "$((SECONDS - t0)) $(info memory used_memory) $(info stats evicted_keys)" \
    "$(info errorstats errorstat_OOM | sed 's/count=//;s/,.*//')" \
    "$(redis-cli -p "$PORT" dbsize)" >> "$OUT/samples.txt"
done
oom=$(info errorstats errorstat_OOM | sed 's/count=//;s/,.*//')
oom=${oom:-0}
evicted=$(info stats evicted_keys)
writes=$(redis-cli -p "$PORT" info commandstats | tr -d '\r' |
  awk -F'[:=,]' '/^cmdstat_(set|hset|hexpire|lpush|rpush|sadd|zadd):/ { n += $3 } END { print n + 0 }')
fruitless=$(grep -c "did not go down" "$OUT/master/server.log")
line="$LABEL oom=$oom fruitless_logs=$fruitless evicted=$evicted writes=$writes \
oom_per_million=$(awk -v o="$oom" -v w="$writes" 'BEGIN { printf "%.3f", w ? o * 1e6 / w : 0 }') \
peak_over=$(cat "$OUT/peak_over.txt" 2>/dev/null || echo 0) peak_over_held=$(cat "$OUT/peak_over_held.txt" 2>/dev/null || echo 0)"
echo "$line" | tee "$OUT/result.txt"
