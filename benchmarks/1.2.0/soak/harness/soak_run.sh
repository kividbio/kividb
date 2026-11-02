#!/usr/bin/env bash
# soak_run.sh <master-ip> <replica-ip> <rate ops/s> <duration e.g. 150m> <out-dir>     (env REPLICA_EXTRA: directives for the replica only)
# From the client VM: fresh master + replica (server.sh over ssh), prefill, the soak, then a full
# master/replica comparison, 5 more minutes of samples with no load, and the charts.
set -u
M=$1 R=$2 RATE=$3 DUR=$4 OUT=$5
H=$(cd "$(dirname "$0")" && pwd)
mkdir -p "$OUT"
s() { ssh -o StrictHostKeyChecking=no -o LogLevel=ERROR "$@"; }
s "$M" bash server.sh start-master
s "$R" "EXTRA='${REPLICA_EXTRA:-}' bash server.sh start-replica $M"   # REPLICA_EXTRA="appendonly no" for a replica without AOF
sleep 3
s "$M" '/opt/kv/kividb --version' > "$OUT/version.txt"
"$HOME/soakgen" -addr "$M:6379" -conns 64 -prefill
date +%s.%N > "$OUT/soak.start"
"$HOME/soakgen" -addr "$M:6379" -conns 128 -rate "$RATE" -duration "$DUR" > "$OUT/soak.csv" 2> "$OUT/soakgen.err"
date +%s.%N > "$OUT/soak.end"
# replica caught up?
for _ in $(seq 1 120); do
  a=$(redis-cli -h "$M" -p 6379 INFO replication | tr -d '\r' | awk -F: '$1=="master_repl_offset"{print $2}')
  b=$(redis-cli -h "$R" -p 6379 INFO replication | tr -d '\r' | awk -F: '$1=="slave_repl_offset"{print $2}')
  [ -n "$a" ] && [ "$a" = "$b" ] && break; sleep 1
done
echo "offsets master=$a replica=$b" > "$OUT/offsets.txt"
"$HOME/venv/bin/python" "$H/compare.py" "$M:6379" "$R:6379" --out "$OUT/compare.json" > "$OUT/compare.out" 2>&1
sleep 300
for h in "$M" "$R"; do s "$h" 'pkill -f "[s]ampler.sh"; tail -100 /mnt/kv/server.log' > "$OUT/server-$h.log"; done
scp -q -o StrictHostKeyChecking=no "$M:samples-master.csv" "$OUT/"
scp -q -o StrictHostKeyChecking=no "$R:samples-replica.csv" "$OUT/"
"$HOME/venv/bin/python" "$H/soak_report.py" "$OUT" "$OUT/charts" > "$OUT/report.out" 2>&1
echo SOAK-DONE
