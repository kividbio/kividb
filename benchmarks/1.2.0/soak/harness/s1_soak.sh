#!/usr/bin/env bash
# Soak 1, run on the client VM (r7-sc): master + replica, both appendonly yes / appendfsync everysec, everything else
# (aof-ack-after-write, io model, threads) at its default. 120 min of the soakgen mix at 100k ops/s (128 conns, open
# loop) plus writelog.py (8 writers x 100 writes/s, every acknowledged write logged). Events (s1_events.sh): clean
# restart (SIGTERM) of the master at 55 min, kill -9 of the replica at 85 min. At the end: the write log checked on
# both nodes, a full master/replica comparison, 3 quiet minutes, then the restart-from-AOF check (s1_restart.sh).
set -u
. $HOME/hosts.env   # M R
OUT=$HOME/s1; H=$HOME/h/soak/harness; PY=$HOME/venv/bin/python
RATE=${RATE:-100000} DUR=${DUR:-120m} DURS=${DURS:-7200}
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
date +%s.%N > $OUT/soak.start
$PY $HOME/writelog.py run --target $M:6379 --writers 8 --rate 100 --duration $DURS --out $OUT/wl > $OUT/wl-run.out 2>&1 &
WL=$!
bash $HOME/s1_events.sh $OUT > $OUT/events.log 2>&1 &
EV=$!
$HOME/soakgen -addr $M:6379 -conns 128 -rate $RATE -duration $DUR -err-log $OUT/soak-errs.log > $OUT/soak.csv 2> $OUT/soakgen.err
date +%s.%N > $OUT/soak.end
wait $EV; wait $WL
for _ in $(seq 1 300); do
  a=$(redis-cli -h $M INFO replication | tr -d '\r' | awk -F: '$1=="master_repl_offset"{print $2}')
  b=$(redis-cli -h $R INFO replication | tr -d '\r' | awk -F: '$1=="slave_repl_offset"{print $2}')
  [ -n "$a" ] && [ "$a" = "$b" ] && break; sleep 1
done
echo "offsets master=$a replica=$b" > $OUT/offsets.txt
date +%s.%N > $OUT/verify.start
$PY $HOME/writelog.py verify --addr $M:6379 --name master --out $OUT/wl > $OUT/wl-verify-master.out 2>&1
$PY $HOME/writelog.py verify --addr $R:6379 --name replica --out $OUT/wl > $OUT/wl-verify-replica.out 2>&1
$PY $H/compare.py $M:6379 $R:6379 --out $OUT/compare.json > $OUT/compare.out 2>&1
sleep 180
for x in master:$M replica:$R; do h=${x#*:}; s $h 'cat /mnt/kv/server.log' > $OUT/server-${x%%:*}.log; s $h 'ls -la /mnt/kv' > $OUT/ls-${x%%:*}.txt; done
scp -q $M:samples-master.csv $R:samples-replica.csv $OUT/ 2>/dev/null || { scp -q $M:samples-master.csv $OUT/; scp -q $R:samples-replica.csv $OUT/; }
bash $HOME/s1_restart.sh > $OUT/restart.out 2>&1
for x in master:$M replica:$R; do h=${x#*:}; s $h 'pkill -f "[s]ampler.sh"; cat /mnt/kv/server.log' > $OUT/server-full-${x%%:*}.log; done
echo S1-DONE
