#!/bin/bash
# Memory and write latency during a snapshot and an AOF rewrite (HN: porker, romange). Client-side driver.
#   snapshot.sh <engine> [keys=30000000] [value-bytes=1024]
# Engine with persistence on (AOF on for KiviDB/Redis/Valkey, automatic rewrites off; Dragonfly has no AOF).
# Load <keys> keys, then two windows of 420 s each under a steady write load of 100,000 SET/s
# (100 connections x 1,000/s, pipeline 1, uniformly random overwrites of the loaded keys, same value size):
#   window 1: BGSAVE at +60 s
#   window 2: BGREWRITEAOF at +60 s (skipped for Dragonfly)
# The sampler records memory every second (cgroup anon memory counts a forked child's copied pages);
# memtier records per-second write latency (p50/p99/p99.9/max) in its JSON.
set -uo pipefail
. "$(dirname "$0")/lib.sh"
E=$1; N=${2:-30000000}; V=${3:-1024}
RUN=snap-$E
WIN=${WIN:-420}; PRE=${PRE:-60}
srv /opt/mem/eng.sh stop
srv /opt/mem/eng.sh start "$E" persist | tee "$OUT/$RUN.engine.txt"
srv /opt/mem/mon.sh start "$RUN"
sleep 5
ev load
memtier --protocol=redis --ratio=1:0 --random-data --data-size="$V" --key-pattern=P:P --key-minimum=1 --key-maximum="$N" \
  --requests=allkeys --threads=8 --clients=8 --pipeline=32 --hide-histogram > "$OUT/$RUN-load.memtier.txt" 2>&1
ev loaded
# let a load-time AOF flush settle
sleep 30
writes() {
  memtier --protocol=redis --ratio=1:0 --random-data --data-size="$V" --key-pattern=R:R --distinct-client-seed --key-minimum=1 --key-maximum="$N" \
    --threads=4 --clients=25 --rate-limiting=1000 --test-time=$WIN --print-percentiles=50,99,99.9,100 \
    --hide-histogram --json-out-file="$OUT/$RUN-$1.json" > "$OUT/$RUN-$1.memtier.txt" 2>&1
}
for W in bgsave aofrw; do
  if [ "$W" = aofrw ] && [ "$E" = dragonfly ]; then continue; fi
  ev "$W-window-start"
  writes "$W" &
  MT=$!
  sleep $PRE
  if [ "$W" = bgsave ]; then
    ev bgsave-issued; rcli BGSAVE | tee -a "$OUT/$RUN.replies.txt"
  else
    ev aofrw-issued; rcli BGREWRITEAOF | tee -a "$OUT/$RUN.replies.txt"
  fi
  wait_persist_idle
  ev "$W-done"
  rcli info persistence | tr -d '\r' > "$OUT/$RUN-$W.persistence.txt"
  wait $MT
  ev "$W-window-end"
  sleep 20
done
srv "ls -la /mnt/nvme/data" > "$OUT/$RUN.files.txt" 2>&1
ev end
srv /opt/mem/mon.sh stop
fetch "$RUN"
srv /opt/mem/eng.sh stop
