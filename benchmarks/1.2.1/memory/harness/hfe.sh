#!/bin/bash
# Memory of hashes whose fields carry a TTL (1.2.1 changed field-expiry bookkeeping, #531). Client-side driver.
#   [R=<round>] hfe.sh <engine> [hashes] [fields] [value-bytes]      default 1000000 x 10 fields x 16 B
# Fresh engine, no persistence. Stage 1 writes the hashes (HSET h:<i> f0 v ... f9 v) with no TTLs; stage 2 gives
# every field a TTL of 10 hours (HPEXPIRE h:<i> 36000000 FIELDS 10 f0 ... f9); nothing expires during the run.
# After each stage the load pauses 10 s and a row (stage, sampler row) goes to hfe-checkpoints.csv.
# Bytes per field = (used_memory or RSS at the checkpoint - the idle engine's) / (hashes x fields);
# the TTL's own cost per field = stage 2 - stage 1.
set -uo pipefail
. "$(dirname "$0")/lib.sh"
E=$1; N=${2:-1000000}; F=${3:-10}; V=${4:-16}
RUN=hfe-$E-$N-$F${R:+-$R}
GEN=/var/tmp/hfe-gen
mkdir -p $GEN
# RESP command streams, generated once and reused by every run
python3 - "$N" "$F" "$V" "$GEN" <<'PY'
import os, sys
n, f, v, d = int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3]), sys.argv[4]
def cmd(args):
    out = [b"*%d\r\n" % len(args)]
    for a in args:
        out.append(b"$%d\r\n%s\r\n" % (len(a), a))
    return b"".join(out)
p1, p2 = f"{d}/hset-{n}-{f}-{v}.resp", f"{d}/hpexpire-{n}-{f}.resp"
if not os.path.exists(p1):
    val = b"x" * v
    with open(p1 + ".tmp", "wb") as o:
        for i in range(n):
            a = [b"HSET", b"h:%d" % i]
            for j in range(f):
                a += [b"f%d" % j, val]
            o.write(cmd(a))
    os.rename(p1 + ".tmp", p1)
if not os.path.exists(p2):
    with open(p2 + ".tmp", "wb") as o:
        for i in range(n):
            o.write(cmd([b"HPEXPIRE", b"h:%d" % i, b"36000000", b"FIELDS", b"%d" % f] + [b"f%d" % j for j in range(f)]))
    os.rename(p2 + ".tmp", p2)
PY
srv /opt/mem/eng.sh stop
srv /opt/mem/eng.sh start "$E" plain | tee "$OUT/$RUN.engine.txt"
srv /opt/mem/mon.sh start "$RUN"
sleep 5
ev start
B=$(srv "tail -1 /var/tmp/mem/$RUN.csv")
echo "$E,$N,$F,$V,${R:-r1},idle,$B" >> "$OUT/hfe-checkpoints.csv"
for S in hset hpexpire; do
  [ $S = hset ] && P=$GEN/hset-$N-$F-$V.resp || P=$GEN/hpexpire-$N-$F.resp
  ev "load-$S"
  redis-cli -h "$SRV" -p 6379 --pipe < "$P" > "$OUT/$RUN-$S.pipe.txt" 2>&1
  ev "loaded-$S"
  sleep 10
  L=$(srv "tail -1 /var/tmp/mem/$RUN.csv")
  echo "$E,$N,$F,$V,${R:-r1},$S,$L" >> "$OUT/hfe-checkpoints.csv"
  echo "  after $S: $(tail -2 "$OUT/$RUN-$S.pipe.txt" | tr '\n' ' ') :: $L"
done
# a spot check that the TTLs are there: the TTL of one field of the first and the last hash
echo "httl: $(rcli HPTTL h:0 FIELDS 1 f0 | tr '\n' ' ') $(rcli HPTTL "h:$((N - 1))" FIELDS 1 "f$((F - 1))" | tr '\n' ' ')" | tee -a "$OUT/$RUN.engine.txt"
rcli info memory | tr -d '\r' | grep -E '^(used_memory|used_memory_rss|mem_fragmentation_ratio):' >> "$OUT/$RUN.engine.txt"
rcli info keyspace | tr -d '\r' | grep '^db' >> "$OUT/$RUN.engine.txt"
ev end
srv /opt/mem/mon.sh stop
fetch "$RUN"
srv /opt/mem/eng.sh stop
