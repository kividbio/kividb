#!/usr/bin/env bash
# r303load.sh <stop|never> <delay ms> <rep> <outdir>: #303 (two engines in one data directory) under write load, AOF on.
#
# Engine A (master, port 7100, appendonly yes, everysec, aof-ack-after-write yes, save 3600 1) gets 300,000 keys and a
# SAVE (an old dump.kdb), then 16 ackcheck.py writers write unique keys to it for the rest of the run. A replica R
# (own directory, port 7200) follows A. At t = 8 s engine B (port 7101) is started in A's data directory.
#   stop:  <delay> ms later A gets SIGTERM, as a kubelet does after a forced pod delete, while the writers go on.
#          Expected: B logs that the directory is in use, waits for A to exit, then loads everything: every write A
#          acknowledged is in B (0 lost, 0 wrong). R is then pointed at B and must end with B's dataset.
#   never: A is never stopped. Expected: after 30 s B is still waiting (alive, not listening, no file of A's
#          directory open but the lock), A goes on acknowledging writes, and every acknowledged write is in A.
#          Then A gets SIGTERM, and B must start and hold every acknowledged write.
set -u
MODE=$1 D=$2 REP=$3 O=$4
BIN=${BIN:-/opt/kv/kividb} PY=$HOME/venv/bin/python H=$(cd "$(dirname "$0")" && pwd)
W=/tmp/r303l OUT=$O/$MODE-${D}ms-$REP
OLD=300000
pkill -9 -x kividb 2>/dev/null; sleep 0.5
rm -rf $W "$OUT"; mkdir -p $W/a $W/r "$OUT"
printf 'save 3600 1\nappendonly yes\nappendfsync everysec\naof-ack-after-write yes\n' > $W/kividb.conf
EV=$OUT/events; : > $EV
ev() { echo "$(date +%s.%N) $*" >> $EV; }
wait_up() { for _ in $(seq ${2:-600}); do redis-cli -p $1 ping 2>/dev/null | grep -q PONG && return 0; sleep 0.1; done; return 1; }
info() { redis-cli -p $1 info replication | tr -d '\r' | grep "^$2:" | cut -d: -f2; }
fill() { python3 "$H/r303fill.py" $1 $2 $3 | redis-cli -p $1 --pipe > /dev/null; }
at() { local t0; t0=$(cat $OUT/t0 2>/dev/null); while [ -z "$t0" ]; do sleep 0.05; t0=$(cat $OUT/t0 2>/dev/null); done
       sleep "$(awk -v a=$t0 -v b=$1 -v c=$(date +%s.%N) 'BEGIN{x=a+b-c; printf "%.3f", (x>0?x:0)}')"; }

(cd $W/a && exec $BIN --port 7100 --bind 127.0.0.1 --configfile $W/kividb.conf > $W/a.log 2>&1) &
A=$!; wait_up 7100 || { echo "A did not start"; exit 1; }
(cd $W/r && exec $BIN --port 7200 --bind 127.0.0.1 --configfile $W/kividb.conf --replicaof 127.0.0.1 7100 > $W/r.log 2>&1) &
R=$!; wait_up 7200
fill 7100 old $OLD; redis-cli -p 7100 save > /dev/null; ev "old dump.kdb saved ($OLD keys)"
echo 127.0.0.1:7100 > $OUT/target
if [ $MODE = stop ]; then DUR=30 VA=127.0.0.1:7101; else DUR=45 VA=127.0.0.1:7100; fi
$PY $H/ackcheck.py --mode standalone --target-file $OUT/target --verify-addr $VA --writers 16 --duration $DUR --timeout 1 \
  --out $OUT --events $EV > $OUT/ackcheck.out 2>&1 &
CK=$!
at 8
(cd $W/a && exec $BIN --port 7101 --bind 127.0.0.1 --configfile $W/kividb.conf > $W/b.log 2>&1) &
B=$!; ev "B started in A's data directory"
if [ $MODE = stop ]; then
  sleep "$(awk -v d=$D 'BEGIN{print d/1000}')"
  kill -TERM $A; ev "SIGTERM to A"; t0=$(date +%s.%N)
  while kill -0 $A 2>/dev/null; do sleep 0.02; done; ev "A exited"
  a_exit=$(awk -v t=$t0 -v n=$(date +%s.%N) 'BEGIN{printf "%.2f", n-t}')
  if wait_up 7101 600; then ev "B answers PING"; else ev "B did not answer within 60 s"; fi
  wait $CK
  b_keys=$(redis-cli -p 7101 dbsize)
  # R: point it at B; it must end with B's dataset
  redis-cli -p 7200 replicaof 127.0.0.1 7101 > /dev/null
  for _ in $(seq 600); do [ "$(info 7200 master_link_status)" = up ] && [ "$(info 7200 slave_repl_offset)" = "$(info 7101 master_repl_offset)" ] && break; sleep 0.1; done
  r_keys=$(redis-cli -p 7200 dbsize)
  r_sync=$(grep -i -E 'resync|continued|partial|full sync' $W/r.log | tail -1 | cut -c26-150)
else
  at 38
  b_alive=$(kill -0 $B 2>/dev/null && echo yes || echo no)
  b_listen=$(ss -ltn 2>/dev/null | grep -c ":7101 ")
  b_ping=$(timeout 2 redis-cli -p 7101 ping 2>&1 | head -1 | cut -c1-60)
  b_fds=$(ls -l /proc/$B/fd 2>/dev/null | grep -o "$W/a/[^ ]*" | sort -u | tr '\n' ' ')
  b_waitlog=$(grep -c "in use by another process" $W/b.log)
  b_lastlog=$(tail -1 $W/b.log | cut -c1-200)
  ls -l --time-style=+%T $W/a > $OUT/a-dir-while-B-waits.txt
  ev "B checked while waiting"
  wait $CK
  a_lost=$(jq -r '"\(.lost_acked) lost, \(.wrong_value) wrong of \(.acked) acked"' $OUT/summary.json)
  kill -TERM $A; ev "SIGTERM to A"; t0=$(date +%s.%N)
  while kill -0 $A 2>/dev/null; do sleep 0.02; done; ev "A exited"
  a_exit=$(awk -v t=$t0 -v n=$(date +%s.%N) 'BEGIN{printf "%.2f", n-t}')
  if wait_up 7101 600; then ev "B answers PING"; else ev "B did not answer within 60 s"; fi
  mkdir -p $OUT/b; cp $OUT/acked.json $OUT/b/
  $PY $H/ackcheck.py --mode standalone --verify-addr 127.0.0.1:7101 --verify-only --duration $DUR --out $OUT/b > $OUT/b/ackcheck.out 2>&1
  b_keys=$(redis-cli -p 7101 dbsize)
fi
cp $W/a.log $W/b.log $W/r.log $OUT/
SJ=$OUT/summary.json; [ $MODE = never ] && SJ=$OUT/b/summary.json
acked=$(jq .acked $SJ) lost=$(jq .lost_acked $SJ) wrong=$(jq .wrong_value $SJ) unk=$(jq .unacked_errors $SJ)
extra=$((b_keys - OLD - acked))
waited=$(grep -c "in use by another process" $W/b.log)
free=$(grep -o -E "free after [0-9.]+s" $W/b.log | head -1)
claims=$(grep -c "can be continued" $W/b.log)
loaded=$(grep -o -E "Loaded [0-9]+ keys[^)]*[)]|Replay[a-z]* [^,]*" $W/b.log | head -3 | tr '\n' ';' | cut -c1-200)
v=PASS
if [ "$lost" != 0 ] || [ "$wrong" != 0 ] || [ $extra -lt 0 ] || [ $extra -gt $unk ] || [ "$waited" = 0 ]; then v=FAIL; fi
if [ $MODE = stop ]; then
  [ "$r_keys" != "$b_keys" ] && v=FAIL
  echo "[$MODE delay=${D}ms rep$REP] acked=$acked lost=$lost wrong=$wrong unacked_errors=$unk B_keys=$b_keys (old $OLD + acked + $extra unconfirmed) A_exit_after_SIGTERM=${a_exit}s B_waited=$waited ($free) B_loaded=[$loaded] B_claims_A_stream=$claims R_after_replicaof_B: keys=$r_keys sync=[$r_sync] -> $v" | tee $OUT/verdict.txt
else
  if [ $b_alive != yes ] || [ $b_listen != 0 ] || [ "$b_ping" = PONG ]; then v=FAIL; fi
  echo "[$MODE rep$REP] after 30 s: B alive=$b_alive listening=$b_listen ping=[$b_ping] B's open files in A's dir=[$b_fds] wait-log lines=$b_waitlog last=[$b_lastlog]; A meanwhile: $a_lost; then SIGTERM A: exit after ${a_exit}s, B ($free) keys=$b_keys, verify on B: acked=$acked lost=$lost wrong=$wrong ($extra unconfirmed) -> $v" | tee $OUT/verdict.txt
fi
{ pkill -9 -x kividb; sleep 0.5; } 2>/dev/null
