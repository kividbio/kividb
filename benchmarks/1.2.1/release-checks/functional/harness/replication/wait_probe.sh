#!/usr/bin/env bash
# wait_probe.sh <binary>: master :6601 + replica :6602 on this host, under a light writer; kill -9 the master,
# REPLICAOF NO ONE on the replica, then time SET + WAIT 1 200 on the promoted node, 20 times.
B=$1; D=$(mktemp -d)
for p in 6601 6602; do mkdir -p $D/$p; printf 'appendonly yes\nappendfsync everysec\n' > $D/$p/c.conf; done
echo "replicaof 127.0.0.1 6601" >> $D/6602/c.conf
for p in 6601 6602; do (cd $D/$p && exec $B --port $p --dir $D/$p --configfile $D/$p/c.conf >> $D/$p/log 2>&1) & echo $! > $D/$p.pid; done
sleep 2; for i in $(seq 50); do redis-cli -p 6602 INFO replication | grep -q 'master_link_status:up' && break; sleep 0.2; done
for i in $(seq 2000); do echo "SET k$i v"; done | redis-cli -p 6601 > /dev/null
echo "before: $( (time -p (redis-cli -p 6601 SET a 1 >/dev/null; redis-cli -p 6601 WAIT 1 200)) 2>&1 | tr '\n' ' ')"
kill -9 $(cat $D/6601.pid); sleep 0.7
redis-cli -p 6602 REPLICAOF NO ONE
for i in $(seq 1 20); do
  s=$(date +%s%N); r=$(redis-cli -p 6602 SET x$i 1); w=$(redis-cli -p 6602 WAIT 1 200); e=$(date +%s%N)
  echo "t+$i: SET=$r WAIT=$w $(( (e - s) / 1000000 )) ms"
done
grep -iE "warn|error|master|replica" $D/6602/log | tail -8
kill -9 $(cat $D/6602.pid); rm -rf $D
