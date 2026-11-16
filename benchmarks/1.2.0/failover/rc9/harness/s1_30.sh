#!/usr/bin/env bash
# Item 6, run on the soak client (sc): the rc8 standalone sanity run (s1_sanity.sh) at 30 min, with events and ackcheck.
# Master + replica, both appendonly yes / appendfsync everysec, everything else at its default. 1 M string keys
# prefilled; the rc7 soak mix at 100k ops/s (128 conns, open loop); writelog.py (8 writers x 100 writes/s, eight
# kinds of write incl. MULTI/EXEC and field TTLs) and ackcheck.py (standalone, 8 writers x 100 SET/s) alongside.
# Events (s1_events.sh): SIGTERM restart of the master at 10 min, kill -9 of the replica at 20 min (started again
# 20 s later). At the end: the replica's offset must reach the master's; the write log and ackcheck are checked on
# both nodes; compare.py compares master and replica (full scan, type-aware, hash-field-TTL allowance).
set -u
. $HOME/hosts.env   # M R
OUT=${OUT:-$HOME/s1}; H=$HOME/h/soak/harness; PY=$HOME/venv/bin/python
RATE=${RATE:-100000} DUR=${DUR:-30m} DURS=${DURS:-1800}
rm -rf $OUT; mkdir -p $OUT
s() { for i in 1 2 3 4 5 6; do ssh -o StrictHostKeyChecking=no -o LogLevel=ERROR "$@"; rc=$?; [ $rc != 255 ] && return $rc; sleep 3; done; return 255; }
s $M bash server.sh start-master
s $R "bash server.sh start-replica $M"
sleep 3
s $M '/opt/kv/kividb --version; sha256sum /opt/kv/kividb; cat /mnt/kv/node.conf' > $OUT/version.txt
s $R 'cat /mnt/kv/node.conf' > $OUT/replica-node.conf
redis-cli -h $M CONFIG GET aof-ack-after-write >> $OUT/version.txt
redis-cli -h $M CONFIG GET appendfsync >> $OUT/version.txt
redis-cli -h $M CONFIG GET repl-backlog-size >> $OUT/version.txt
$HOME/soakgen -addr $M:6379 -conns 64 -prefill
echo "$M:6379" > $OUT/target
: > $OUT/events
date +%s.%N > $OUT/soak.start
$PY $HOME/writelog.py run --target $M:6379 --writers 8 --rate 100 --duration $DURS --out $OUT/wl > $OUT/wl-run.out 2>&1 &
WL=$!
$PY $HOME/ackcheck.py --mode standalone --target-file $OUT/target --writers 8 --rate 100 --duration $DURS \
  --out $OUT/ack --events $OUT/events > $OUT/ack-run.out 2>&1 &
AK=$!
T1=10 T2=20 bash $HOME/s1_events.sh $OUT > $OUT/events.log 2>&1 &
EV=$!
$HOME/soakgen -addr $M:6379 -conns 128 -rate $RATE -duration $DUR -err-log $OUT/soak-errs.log > $OUT/soak.csv 2> $OUT/soakgen.err
date +%s.%N > $OUT/soak.end
wait $EV; wait $WL; wait $AK
for _ in $(seq 1 300); do
  a=$(redis-cli -h $M INFO replication | tr -d '\r' | awk -F: '$1=="master_repl_offset"{print $2}')
  b=$(redis-cli -h $R INFO replication | tr -d '\r' | awk -F: '$1=="slave_repl_offset"{print $2}')
  [ -n "$a" ] && [ "$a" = "$b" ] && break; sleep 1
done
echo "offsets master=$a replica=$b" > $OUT/offsets.txt
redis-cli -h $M INFO stats | tr -d '\r' | grep -E '^sync_(full|partial)' > $OUT/sync-stats.txt
date +%s.%N > $OUT/verify.start
$PY $HOME/writelog.py verify --addr $M:6379 --name master --out $OUT/wl > $OUT/wl-verify-master.out 2>&1
$PY $HOME/writelog.py verify --addr $R:6379 --name replica --out $OUT/wl > $OUT/wl-verify-replica.out 2>&1
rm -rf $OUT/ack-replica; mkdir -p $OUT/ack-replica; cp $OUT/ack/acked.json $OUT/ack-replica/
$PY $HOME/ackcheck.py --mode standalone --verify-only --verify-addr $R:6379 --duration $DURS --out $OUT/ack-replica \
  > $OUT/ack-verify-replica.out 2>&1
$PY $H/compare.py $M:6379 $R:6379 --out $OUT/compare.json > $OUT/compare.out 2>&1
for x in master:$M replica:$R; do h=${x#*:}; s $h 'pkill -f "[s]ampler.sh"; cat /mnt/kv/server.log' > $OUT/server-${x%%:*}.log; s $h 'ls -la /mnt/kv' > $OUT/ls-${x%%:*}.txt; done
scp -q $M:samples-master.csv $OUT/; scp -q $R:samples-replica.csv $OUT/
echo S1-DONE
