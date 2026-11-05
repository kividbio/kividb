#!/usr/bin/env bash
# Standalone sanity (rc8), run on the client VM (r8-sc): s1_soak.sh cut down to 20 min with no events.
# Master + replica, both appendonly yes / appendfsync everysec, everything else at its default. The rc7 soak mix at
# 100k ops/s (128 conns, open loop) plus writelog.py (8 writers x 100 writes/s, every acknowledged write logged).
# At the end: the replica's offset must reach the master's, the write log is checked on both nodes, and compare.py
# compares master and replica (full scan, type-aware).
set -u
. $HOME/hosts.env   # M R
OUT=${OUT:-$HOME/s1}; H=$HOME/h/soak/harness; PY=$HOME/venv/bin/python
RATE=${RATE:-100000} DUR=${DUR:-20m} DURS=${DURS:-1200}
rm -rf $OUT; mkdir -p $OUT
s() { for i in 1 2 3 4 5 6; do ssh -o StrictHostKeyChecking=no -o LogLevel=ERROR "$@"; rc=$?; [ $rc != 255 ] && return $rc; sleep 3; done; return 255; }
s $M bash server.sh start-master
s $R "bash server.sh start-replica $M"
sleep 3
s $M '/opt/kv/kividb --version; sha256sum /opt/kv/kividb; cat /mnt/kv/node.conf' > $OUT/version.txt
s $R 'cat /mnt/kv/node.conf' > $OUT/replica-node.conf
redis-cli -h $M CONFIG GET aof-ack-after-write >> $OUT/version.txt
redis-cli -h $M CONFIG GET appendfsync >> $OUT/version.txt
$HOME/soakgen -addr $M:6379 -conns 64 -prefill
: > $OUT/events
date +%s.%N > $OUT/soak.start
$PY $HOME/writelog.py run --target $M:6379 --writers 8 --rate 100 --duration $DURS --out $OUT/wl > $OUT/wl-run.out 2>&1 &
WL=$!
$HOME/soakgen -addr $M:6379 -conns 128 -rate $RATE -duration $DUR -err-log $OUT/soak-errs.log > $OUT/soak.csv 2> $OUT/soakgen.err
date +%s.%N > $OUT/soak.end
wait $WL
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
$PY $H/compare.py $M:6379 $R:6379 --out $OUT/compare.json > $OUT/compare.out 2>&1
for x in master:$M replica:$R; do h=${x#*:}; s $h 'pkill -f "[s]ampler.sh"; cat /mnt/kv/server.log' > $OUT/server-${x%%:*}.log; s $h 'ls -la /mnt/kv' > $OUT/ls-${x%%:*}.txt; done
scp -q $M:samples-master.csv $OUT/; scp -q $R:samples-replica.csv $OUT/
echo S1-DONE
