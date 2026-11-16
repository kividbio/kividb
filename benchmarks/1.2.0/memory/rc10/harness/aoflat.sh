#!/bin/bash
# Write latency with the AOF on, outside any snapshot: the check of a candidate-vs-rc4 difference seen in the
# snapshot runs' steady state. Client-side driver.
#   [R=<round>] [SECS=180] aoflat.sh <engine> [keys=2000000] [value-bytes=1024]
# Fresh engine with persistence on (eng.sh profile "persist": AOF on, automatic rewrites off). Load <keys> keys,
# wait 30 s, then 100,000 SET/s (100 connections x 1,000/s, pipeline 1, uniformly random overwrites, same size,
# distinct client seeds) for SECS seconds: the snapshot test's write load without the snapshot.
# PERF=1 also records `perf record -g` of the engine for 30 s in the middle of the window.
set -uo pipefail
. "$(dirname "$0")/lib.sh"
E=$1; N=${2:-2000000}; V=${3:-1024}
RUN=aoflat-$E${R:+-$R}
SECS=${SECS:-180}
srv /opt/mem/eng.sh stop
srv /opt/mem/eng.sh start "$E" persist | tee "$OUT/$RUN.engine.txt"
srv /opt/mem/mon.sh start "$RUN"
sleep 5
ev load
memtier --protocol=redis --ratio=1:0 --random-data --data-size="$V" --key-pattern=P:P --key-minimum=1 --key-maximum="$N" \
  --requests=allkeys --threads=8 --clients=8 --pipeline=32 --hide-histogram > "$OUT/$RUN-load.memtier.txt" 2>&1
ev loaded
sleep 30
ev writes-start
memtier --protocol=redis --ratio=1:0 --random-data --data-size="$V" --key-pattern=R:R --distinct-client-seed --key-minimum=1 \
  --key-maximum="$N" --threads=4 --clients=25 --rate-limiting=1000 --test-time="$SECS" --print-percentiles=50,99,99.9,100 \
  --hide-histogram --json-out-file="$OUT/$RUN.json" > "$OUT/$RUN.memtier.txt" 2>&1 &
MT=$!
if [ "${PERF:-0}" = 1 ]; then
  sleep $((SECS / 2 - 15))
  srv "cd /var/tmp/mem && sudo perf record -F 999 -g -p \$(/opt/mem/eng.sh pid) -o $RUN.perf.data -- sleep 30 > /dev/null 2>&1; sudo perf report -i $RUN.perf.data --no-children --stdio --percent-limit 0.5 2>/dev/null | head -n 400 > $RUN.perf.txt"
  scp -q -i "$HOME/.ssh/id_mem" -o StrictHostKeyChecking=no "azureuser@$SRV:/var/tmp/mem/$RUN.perf.txt" "$OUT/" || true
fi
wait $MT
ev writes-end
srv "ps -o times= -p \$(/opt/mem/eng.sh pid)" > "$OUT/$RUN.cpu.txt" 2>&1
ev end
srv /opt/mem/mon.sh stop
fetch "$RUN"
srv /opt/mem/eng.sh stop
