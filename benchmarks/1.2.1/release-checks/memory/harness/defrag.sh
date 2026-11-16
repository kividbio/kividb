#!/bin/bash
# Active defrag after a mass delete (KiviDB). Client-side driver.
#   [R=<round>] [TAG=<label>] defrag.sh <engine> [keys=5000000] [value-bytes=100] [extra engine args, e.g. --activedefrag no]
# Fresh engine, no persistence, the engine's default active-defrag settings unless extra args say otherwise
# (KiviDB 1.2: activedefrag yes, ignore 64 MB, threshold-lower 20%, cycle-max 10% of one core).
# Load <keys> keys of <value-bytes> random bytes (memtier-1..N), plus 100k probe-* keys of the same size.
# Then a fixed probe load runs for the whole measurement (PROBE seconds, default 600):
#   GET  80,000/s (80 connections x 1,000/s) over the whole keyspace: half of them miss after the delete
#   SET  20,000/s (20 connections x 1,000/s) overwriting the probe-* keys, same size (no new memory)
# DEL_AT seconds in (default 60), a random half of memtier-1..N is deleted (fixed seed: the same keys for every
# engine and round), 1,000 DELs per pipeline batch through redis-cli --pipe. The sampler records RSS,
# used_memory, the slabs' bytes and live bytes, and active_defrag_running every second; after the probe load
# ends, sampling goes on until active_defrag_running has been 0 for 30 s (at most 10 more minutes).
set -uo pipefail
. "$(dirname "$0")/lib.sh"
E=$1; N=${2:-5000000}; V=${3:-100}; shift; shift || true; shift || true
EXTRA="$*"
RUN=defrag-$E${TAG:+-$TAG}${R:+-$R}
PROBE=${PROBE:-600}; DEL_AT=${DEL_AT:-60}
srv /opt/mem/eng.sh stop
srv /opt/mem/eng.sh start "$E" plain $EXTRA | tee "$OUT/$RUN.engine.txt"
rcli CONFIG GET 'active*' | tr '\n' ' ' >> "$OUT/$RUN.engine.txt"; echo >> "$OUT/$RUN.engine.txt"
srv /opt/mem/mon.sh start "$RUN"
sleep 5
# the delete list: the same random half for every run (seeded), as a RESP stream
DELS=$OUT/dels-$N.resp
[ -s "$DELS" ] || python3 - "$N" "$DELS" <<'PY'
import random, sys
n, out = int(sys.argv[1]), sys.argv[2]
ks = random.Random(20261102).sample(range(1, n + 1), n // 2)
with open(out, "wb") as f:
    for i in range(0, len(ks), 1000):
        part = [b"memtier-%d" % k for k in ks[i:i + 1000]]
        f.write(b"*%d\r\n$3\r\nDEL\r\n" % (len(part) + 1))
        for k in part:
            f.write(b"$%d\r\n%s\r\n" % (len(k), k))
PY
ev load
memtier --protocol=redis --ratio=1:0 --random-data --data-size="$V" --key-pattern=P:P --key-minimum=1 --key-maximum="$N" \
  --requests=allkeys --threads=8 --clients=8 --pipeline=32 --hide-histogram > "$OUT/$RUN-load.memtier.txt" 2>&1
memtier --protocol=redis --ratio=1:0 --random-data --data-size="$V" --key-prefix=probe- --key-pattern=P:P --key-minimum=1 \
  --key-maximum=100000 --requests=allkeys --threads=2 --clients=4 --pipeline=32 --hide-histogram >> "$OUT/$RUN-load.memtier.txt" 2>&1
ev loaded
sleep 20
ev probe-start
memtier --protocol=redis --ratio=0:1 --key-pattern=R:R --distinct-client-seed --key-minimum=1 --key-maximum="$N" \
  --threads=4 --clients=20 --rate-limiting=1000 --test-time="$PROBE" --print-percentiles=50,99,99.9,100 \
  --hide-histogram --json-out-file="$OUT/$RUN-get.json" > "$OUT/$RUN-get.memtier.txt" 2>&1 &
G=$!
memtier --protocol=redis --ratio=1:0 --random-data --data-size="$V" --key-prefix=probe- --key-pattern=R:R --distinct-client-seed \
  --key-minimum=1 --key-maximum=100000 --threads=2 --clients=10 --rate-limiting=1000 --test-time="$PROBE" \
  --print-percentiles=50,99,99.9,100 --hide-histogram --json-out-file="$OUT/$RUN-set.json" > "$OUT/$RUN-set.memtier.txt" 2>&1 &
S=$!
sleep "$DEL_AT"
ev delete-start
redis-cli -h "$SRV" -p 6379 --pipe < "$DELS" > "$OUT/$RUN.delete.txt" 2>&1
ev delete-done
rcli DBSIZE | tee -a "$OUT/$RUN.delete.txt"
wait $G $S
ev probe-end
idle=0
for _ in $(seq 1 600); do
  r=$(rcli INFO memory | tr -d '\r' | awk -F: '/^active_defrag_running:/{print $2}')
  if [ "${r:-0}" = 0 ]; then idle=$((idle + 1)); [ $idle -ge 30 ] && break; else idle=0; fi
  sleep 1
done
ev end
rcli INFO memory | tr -d '\r' > "$OUT/$RUN.info-end.txt"
rcli INFO stats | tr -d '\r' | grep -E 'defrag' >> "$OUT/$RUN.info-end.txt"
srv /opt/mem/mon.sh stop
fetch "$RUN"
srv /opt/mem/eng.sh stop
