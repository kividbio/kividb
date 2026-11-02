#!/bin/bash
# Memory over time under a mixed workload (HN: kingcauchy). Client-side driver.
#   mixed.sh <engine> [seconds=1200] [keys=10000000]
# Fresh engine, no persistence. Load <keys> keys of 100 B, then for <seconds> run, at a fixed 150,000 ops/s
# (100 connections x 1,500/s, pipeline 1, the same offered load for every engine):
#   80% GET / 20% SET, uniformly random keys over the whole keyspace,
#   every SET an overwrite or re-create with a random size of 64-1024 B and a TTL of 60-600 s
#   (memtier --expiry-range), so values change size, keys expire and come back.
# Memory is sampled every second; memtier writes per-second latency percentiles to its JSON.
set -uo pipefail
. "$(dirname "$0")/lib.sh"
E=$1; SECS=${2:-1200}; N=${3:-10000000}
RUN=mixed-$E
srv /opt/mem/eng.sh stop
srv /opt/mem/eng.sh start "$E" plain | tee "$OUT/$RUN.engine.txt"
srv /opt/mem/mon.sh start "$RUN"
sleep 5
ev load
memtier --protocol=redis --ratio=1:0 --random-data --data-size=100 --key-pattern=P:P --key-minimum=1 --key-maximum="$N" \
  --requests=allkeys --threads=8 --clients=8 --pipeline=32 --hide-histogram > "$OUT/$RUN-load.memtier.txt" 2>&1
ev loaded
sleep 30
ev mixed-start
memtier --protocol=redis --ratio=1:4 --key-pattern=R:R --distinct-client-seed --key-minimum=1 --key-maximum="$N" \
  --random-data --data-size-range=64-1024 --expiry-range=60-600 --threads=4 --clients=25 --rate-limiting=1500 \
  --test-time="$SECS" --print-percentiles=50,99,99.9 --hide-histogram \
  --json-out-file="$OUT/$RUN.json" > "$OUT/$RUN.memtier.txt" 2>&1
ev mixed-end
sleep 30
ev end
srv /opt/mem/mon.sh stop
fetch "$RUN"
srv /opt/mem/eng.sh stop
