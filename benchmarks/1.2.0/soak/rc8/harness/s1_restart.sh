#!/usr/bin/env bash
# After soak 1 (the rc6 soak's s1restart.sh, plus the write log): restart both nodes from their own AOFs with kill -9,
# and check the data against the other node, and the write log against each.
#  1. REPLICAOF NO ONE on the replica (it keeps its data and its AOF; a restarted master must not resync it)
#  2. compare master vs replica (baseline)
#  3. kill -9 the master, restart it from /mnt/kv (same node.conf), compare against the untouched replica, write log
#  4. kill -9 the replica, restart it from its AOF without replicaof, compare against the restarted master, write log
set -u
. $HOME/hosts.env
OUT=$HOME/s1/restart; mkdir -p $OUT
PY=$HOME/venv/bin/python; CMP=$HOME/h/soak/harness/compare.py
s() { for i in 1 2 3 4 5 6; do ssh -o StrictHostKeyChecking=no -o LogLevel=ERROR "$@"; rc=$?; [ $rc != 255 ] && return $rc; sleep 3; done; return 255; }
ks() { redis-cli -h $1 INFO keyspace | tr -d '\r' | awk -F'[:=,]' '/^db0:/{print "keys="$3" expires="$5" persistent="$3-$5}'; }
st() { echo "$(date -u +%T) $*" | tee -a $OUT/steps.txt; echo "$(date +%s.%N) $*" >> $HOME/s1/events; }
el() { awk -v a=$1 -v b=$(date +%s.%N) 'BEGIN{printf "%.2f", b-a}'; }
st "restart-check replica REPLICAOF NO ONE: $(redis-cli -h $R REPLICAOF NO ONE)"
sleep 3
st "restart-check before: master $(ks $M) | replica $(ks $R)"
$PY $CMP $M:6379 $R:6379 --out $OUT/compare-0-before.json > $OUT/compare-0-before.out 2>&1; st "restart-check compare before: $(tail -1 $OUT/compare-0-before.out)"
restart() { # host label
  local h=$1
  sleep 2   # past one everysec fsync
  st "restart-check $2: kill -9 $(s $h 'cat /mnt/kv/pid')"
  s $h 'kill -9 $(cat /mnt/kv/pid); sleep 1; sed -i "/^replicaof/d" /mnt/kv/node.conf; echo "=== restart after kill -9 $(date -u +%FT%TZ)" >> /mnt/kv/server.log; ls -la /mnt/kv > /mnt/kv/ls-before-restart.txt
    (cd /mnt/kv && exec nohup /opt/kv/kividb --configfile /mnt/kv/node.conf >> /mnt/kv/server.log 2>&1 < /dev/null) & echo $! > /mnt/kv/pid'
  local t0=$(date +%s.%N)
  for _ in $(seq 1 1200); do [ "$(redis-cli -h $h PING 2>/dev/null)" = PONG ] && [ "$(redis-cli -h $h INFO persistence | tr -d '\r' | awk -F: '$1=="loading"{print $2}')" = 0 ] && break; sleep 0.1; done
  st "restart-check $2: serving again after $(el $t0) s; $(ks $h)"
  s $h 'sed -n "/=== restart after kill -9/,\$p" /mnt/kv/server.log | grep -i -E "aof|load|replay|kdb|keys" | head -12' | sed 's/^/    log: /' | tee -a $OUT/steps.txt
}
restart $M master
$PY $CMP $M:6379 $R:6379 --out $OUT/compare-1-master-restarted.json > $OUT/compare-1.out 2>&1; st "restart-check compare restarted master vs untouched replica: $(tail -1 $OUT/compare-1.out)"
$PY $HOME/writelog.py verify --addr $M:6379 --name master-after-kill9 --out $HOME/s1/wl > $OUT/wl-master.out 2>&1; st "restart-check write log on the restarted master: $(tail -1 $OUT/wl-master.out)"
restart $R replica
$PY $CMP $M:6379 $R:6379 --out $OUT/compare-2-both-restarted.json > $OUT/compare-2.out 2>&1; st "restart-check compare both restarted: $(tail -1 $OUT/compare-2.out)"
$PY $HOME/writelog.py verify --addr $R:6379 --name replica-after-kill9 --out $HOME/s1/wl > $OUT/wl-replica.out 2>&1; st "restart-check write log on the restarted replica: $(tail -1 $OUT/wl-replica.out)"
st "restart-check after: master $(ks $M) | replica $(ks $R)"
echo RESTART-DONE
