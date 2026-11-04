#!/usr/bin/env bash
# cl_soak.sh, run on the cluster client VM (r10-cc): 3 shards x (1 master + 1 replica) on r10-c1..c3 (ports 7001/7002,
# appendonly yes / everysec, threads 4, data on NVMe), the soakgen mix at 100k ops/s through go-redis ClusterClient,
# ackcheck.py (16 writers x 100 unique SET/s), owcheck.py (8 writers x 200 overwrites/s, half to the moving slots while
# a reshard runs), three shardprobe write probes (one per shard, 500 SET/s each, keys in slots no event moves), and the
# events of cl_events.sh. At the end: each master against its replica, `redis-cli --cluster check`, the logs.
# The rc8 soak's cl_soak.sh; rc10 adds the shard probes, the slowlog at 10 ms with 10000 entries (KiviDB ships it off;
# for the MIGRATE/SETSLOT/GETKEYSINSLOT times) and
# env IOMODEL (empty: the default, io_uring on these VMs; "tokio": io-model tokio on every node).
#   OUT=~/cl [DUR=120m DURS=7200 SCHED=...] bash cl_soak.sh
set -u
. $HOME/clhosts.env
OUT=${OUT:-$HOME/cl}; rm -rf $OUT; mkdir -p $OUT
export NODES="$C1:7001 $C2:7001 $C3:7001 $C1:7002 $C2:7002 $C3:7002"
PY=$HOME/venv/bin/python
DUR=${DUR:-120m} DURS=${DURS:-7200} PROBE_DUR=${PROBE_DUR:-$DUR}
s() { for i in 1 2 3 4 5 6; do ssh "$@"; rc=$?; [ $rc != 255 ] && return $rc; sleep 3; done; return 255; }
DIR="cluster-enabled yes;cluster-node-timeout 5000;cluster-config-file nodes.conf;threads 4;appendfsync everysec;update-check no;slowlog-log-slower-than 10000;slowlog-max-len 10000"
[ -n "${IOMODEL:-}" ] && DIR="$DIR;io-model $IOMODEL"
for h in $C1 $C2 $C3; do s $h "pkill -f '[s]ampler.sh'; bash node.sh wipeall; bash node.sh start kividb 7001 '$DIR'; bash node.sh start kividb 7002 '$DIR'"; done
s $C1 '/opt/kv/kividb --version; sha256sum /opt/kv/kividb; cat /data/7001/node.conf; grep -m1 "I/O model" /data/7001/server.log' > $OUT/version.txt
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
: > $OUT/hot-slots
$PY $HOME/owcheck.py --seeds $SEEDS --writers 8 --rate 200 --duration $DURS --timeout 2 --hot-file $OUT/hot-slots --out $OUT/ow > $OUT/owcheck.out 2>&1 &
OW=$!
# one write probe per shard: keys in slots 2990-3010, 7990-8010, 12990-13010, which no event moves
PR=""
for x in "a 2990-3010" "b 7990-8010" "c 12990-13010"; do
  set -- $x
  $HOME/shardprobe -addr $SEEDS -slots $2 -label $1 -conns 8 -rate 500 -duration $PROBE_DUR -raw $OUT/probe-$1.raw > $OUT/probe-$1.csv 2> $OUT/probe-$1.err &
  PR="$PR $!"
done
bash $HOME/cl_events.sh $OUT > $OUT/events.log 2>&1 &
EVD=$!
$HOME/soakgen-cl -cluster -addr $SEEDS -conns 128 -rate 100000 -duration $DUR -interval 1s -err-log $OUT/soak-errs.log > $OUT/soak.csv 2> $OUT/soakgen.err
date +%s.%N > $OUT/soak.end
wait $EVD
wait $ACK
wait $OW
wait $PR
$PY $HOME/cltool.py wait-ok > $OUT/end-ok.txt
redis-cli -h $C1 -p 7001 CLUSTER NODES > $OUT/nodes-end.txt
redis-cli --cluster check $C1:7001 > $OUT/check-end.txt 2>&1
# each master against its replica, once the replica has caught up; the three shards in parallel
mkdir -p $OUT/compare
CMP=""
for sl in 3000 8000 13000; do
  read m mid r rid <<< "$($PY $HOME/cltool.py shard $sl)"
  for _ in $(seq 1 120); do a=$(redis-cli -h ${m%:*} -p ${m#*:} INFO replication | tr -d '\r' | awk -F: '$1=="master_repl_offset"{print $2}'); b=$(redis-cli -h ${r%:*} -p ${r#*:} INFO replication | tr -d '\r' | awk -F: '$1=="slave_repl_offset"{print $2}'); [ "$a" = "$b" ] && break; sleep 1; done
  echo "shard of slot $sl: master $m replica $r offsets $a $b" >> $OUT/compare/offsets.txt
  $PY $HOME/compare_ro.py $m $r --out $OUT/compare/shard-$sl.json > $OUT/compare/shard-$sl.out 2>&1 &
  CMP="$CMP $!"
done
wait $CMP
kill $POLL
for hp in $NODES; do echo "$hp $(timeout 2 redis-cli -h ${hp%:*} -p ${hp#*:} INFO stats | tr -d '\r' | awk -F: '$1~/^sync_(full|partial_ok|partial_err)$/{printf "%s=%s ",$1,$2}')"; done > $OUT/syncstats-end.txt
sleep 30
for h in $C1 $C2 $C3; do s $h "pkill -f '[s]ampler.sh'"; for p in 7001 7002; do scp -q $h:/data/$p/server.log $OUT/server-$h-$p.log; scp -q $h:samples-$p.csv $OUT/$h-samples-$p.csv; done; done
grep -c -E '\[ERROR\]|panicked' $OUT/server-*.log > $OUT/error-lines.txt
echo CL-DONE
