#!/bin/bash
# maxmemory + snapshot: does a snapshot under a write load make the engine evict keys that fit? (issue #284)
#   evict.sh <engine> [keys=10000000] [value-bytes=1024] [headroom-pct=10]
# Load <keys> keys with no limit, then set maxmemory to used_memory + <headroom-pct>% with allkeys-lru,
# and run 100,000 SET/s of same-size overwrites (nothing grows) for 300 s with BGSAVE at +60 s.
# No AOF (as in the issue's report); the snapshot goes to the data directory.
# Same-size overwrites need no new memory, so every evicted key is an eviction the snapshot caused.
set -uo pipefail
. "$(dirname "$0")/lib.sh"
E=$1; N=${2:-10000000}; V=${3:-1024}; H=${4:-10}
RUN=evict-$E
srv /opt/mem/eng.sh stop
srv /opt/mem/eng.sh start "$E" plain | tee "$OUT/$RUN.engine.txt"
srv /opt/mem/mon.sh start "$RUN"
sleep 5
ev load
memtier --protocol=redis --ratio=1:0 --random-data --data-size="$V" --key-pattern=P:P --key-minimum=1 --key-maximum="$N" \
  --requests=allkeys --threads=8 --clients=8 --pipeline=32 --hide-histogram > "$OUT/$RUN-load.memtier.txt" 2>&1
ev loaded
sleep 20
USED=$(rcli info memory | tr -d '\r' | awk -F: '/^used_memory:/{print $2}')
MAX=$((USED + USED * H / 100))
rcli CONFIG SET maxmemory-policy allkeys-lru
rcli CONFIG SET maxmemory "$MAX"
echo "used_memory=$USED maxmemory=$MAX" | tee "$OUT/$RUN.limits.txt"
rcli info stats | tr -d '\r' | grep -E '^evicted_keys' | tee -a "$OUT/$RUN.limits.txt"
echo "dbsize_before=$(rcli DBSIZE)" | tee -a "$OUT/$RUN.limits.txt"
ev writes-start
memtier --protocol=redis --ratio=1:0 --random-data --data-size="$V" --key-pattern=R:R --distinct-client-seed --key-minimum=1 --key-maximum="$N" \
  --threads=4 --clients=25 --rate-limiting=1000 --test-time=${WIN:-300} --print-percentiles=50,99,99.9,100 \
  --hide-histogram --json-out-file="$OUT/$RUN.json" > "$OUT/$RUN.memtier.txt" 2>&1 &
MT=$!
sleep ${PRE:-60}
ev bgsave-issued; rcli BGSAVE | tee -a "$OUT/$RUN.replies.txt"
wait_persist_idle
ev bgsave-done
wait $MT
ev writes-end
sleep 5
echo "dbsize_after=$(rcli DBSIZE)" | tee -a "$OUT/$RUN.limits.txt"
rcli info stats | tr -d '\r' | grep -E '^evicted_keys' | tee -a "$OUT/$RUN.limits.txt"
rcli info memory | tr -d '\r' | grep -E '^(used_memory|maxmemory|mem_not_counted_for_evict):' | tee -a "$OUT/$RUN.limits.txt"
ev end
srv /opt/mem/mon.sh stop
fetch "$RUN"
srv /opt/mem/eng.sh stop
