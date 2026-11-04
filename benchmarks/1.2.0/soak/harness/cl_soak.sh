#!/usr/bin/env bash
# Soak 2, run on the cluster client VM (r7-cc): 3 shards x (1 master + 1 replica) on r7-c1..c3 (ports 7001/7002,
# appendonly yes / everysec, threads 4, data on NVMe), the soakgen mix at 100k ops/s through go-redis ClusterClient for
# 120 min, ackcheck.py (cluster mode, 16 writers, 100 SETs/s each) recording every acknowledged write, and the events
# of cl_events.sh (failovers, kill -9 of masters, redis-cli reshards, CLUSTER MIGRATION IMPORTs).
# The rc6 soak's cl_soak.sh with the rc7 schedule.
set -u
. $HOME/clhosts.env
OUT=${OUT:-$HOME/cl}; rm -rf $OUT; mkdir -p $OUT
export NODES="$C1:7001 $C2:7001 $C3:7001 $C1:7002 $C2:7002 $C3:7002"
PY=$HOME/venv/bin/python
DUR=${DUR:-120m} DURS=${DURS:-7200}
s() { for i in 1 2 3 4 5 6; do ssh "$@"; rc=$?; [ $rc != 255 ] && return $rc; sleep 3; done; return 255; }
DIR="cluster-enabled yes;cluster-node-timeout 5000;cluster-config-file nodes.conf;threads 4;appendfsync everysec;update-check no"
for h in $C1 $C2 $C3; do s $h "pkill -f '[s]ampler.sh'; bash node.sh wipeall; bash node.sh start kividb 7001 '$DIR'; bash node.sh start kividb 7002 '$DIR'"; done
s $C1 '/opt/kv/kividb --version; sha256sum /opt/kv/kividb; cat /data/7001/node.conf' > $OUT/version.txt
yes yes | redis-cli --cluster create $NODES --cluster-replicas 1 --cluster-yes > $OUT/create.log 2>&1
$PY $HOME/cltool.py wait-ok > $OUT/create-ok.txt
redis-cli -h $C1 -p 7001 CLUSTER NODES > $OUT/nodes-start.txt
for h in $C1 $C2 $C3; do s $h "rm -f samples-*.csv; for p in 7001 7002; do nohup bash sampler.sh \$p /data/\$p/pid samples-\$p.csv 10 > /dev/null 2>&1 < /dev/null & done"; done
SEEDS=$C1:7001,$C2:7001,$C3:7001
$HOME/soakgen-cl -cluster -addr $SEEDS -conns 64 -prefill 2> $OUT/prefill.err
# cluster state and role of every node, every second
( while :; do for hp in $NODES; do echo "$(date +%s.%N),$hp,$(timeout 1 redis-cli -h ${hp%:*} -p ${hp#*:} CLUSTER INFO 2>/dev/null | tr -d '\r' | awk -F: '$1=="cluster_state"{print $2}'),$(timeout 1 redis-cli -h ${hp%:*} -p ${hp#*:} ROLE 2>/dev/null | head -1)"; done; sleep 1; done ) > $OUT/state.csv &
POLL=$!
date +%s.%N > $OUT/soak.start
$PY $HOME/ackcheck.py --mode cluster --seeds $SEEDS --writers 16 --rate 100 --duration $DURS --timeout 2 --out $OUT/ack --events $OUT/events --settle 10 > $OUT/ackcheck.out 2>&1 &
ACK=$!
bash $HOME/cl_events.sh $OUT > $OUT/events.log 2>&1 &
EVD=$!
$HOME/soakgen-cl -cluster -addr $SEEDS -conns 128 -rate 100000 -duration $DUR -interval 1s -err-log $OUT/soak-errs.log > $OUT/soak.csv 2> $OUT/soakgen.err
date +%s.%N > $OUT/soak.end
wait $EVD
wait $ACK
$PY $HOME/cltool.py wait-ok > $OUT/end-ok.txt
redis-cli -h $C1 -p 7001 CLUSTER NODES > $OUT/nodes-end.txt
redis-cli --cluster check $C1:7001 > $OUT/check-end.txt 2>&1
# each master against its replica, once the replica has caught up; the three shards in parallel
mkdir -p $OUT/compare
for sl in 3000 8000 13000; do
  read m mid r rid <<< "$($PY $HOME/cltool.py shard $sl)"
  for _ in $(seq 1 120); do a=$(redis-cli -h ${m%:*} -p ${m#*:} INFO replication | tr -d '\r' | awk -F: '$1=="master_repl_offset"{print $2}'); b=$(redis-cli -h ${r%:*} -p ${r#*:} INFO replication | tr -d '\r' | awk -F: '$1=="slave_repl_offset"{print $2}'); [ "$a" = "$b" ] && break; sleep 1; done
  echo "shard of slot $sl: master $m replica $r offsets $a $b" >> $OUT/compare/offsets.txt
  $PY $HOME/compare_ro.py $m $r --out $OUT/compare/shard-$sl.json > $OUT/compare/shard-$sl.out 2>&1 &
  CMP="${CMP:-} $!"
done
wait $CMP
kill $POLL
sleep 60
for h in $C1 $C2 $C3; do s $h "pkill -f '[s]ampler.sh'"; for p in 7001 7002; do scp -q $h:/data/$p/server.log $OUT/server-$h-$p.log; scp -q $h:samples-$p.csv $OUT/$h-samples-$p.csv; done; done
echo CL-DONE
