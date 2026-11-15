#!/usr/bin/env bash
# p1_create.sh: check 1. Six nodes (two per VM), redis-cli --cluster create --cluster-replicas 1, then make sure no
# master has its replica on its own VM; --cluster check, CLUSTER INFO, clcheck topo; big collections + 300k bg keys.
set -u
. $HOME/h/lib.sh
OUT=$R/p1; rm -rf $OUT; mkdir -p $OUT; : > $OUT/events
DIR="cluster-enabled yes;cluster-node-timeout 5000;cluster-config-file nodes.conf;threads 4;appendfsync everysec;update-check no;enable-debug-command yes"
for h in $C1 $C2 $C3; do s $h "bash node.sh wipeall; bash node.sh start kividb 7001 '$DIR'; bash node.sh start kividb 7002 '$DIR'"; done
s $C1 '/opt/kv/kividb --version; sha256sum /opt/kv/kividb; cat /data/7001/node.conf; grep -m1 -i "I/O model" /data/7001/server.log' > $OUT/version.txt
t=$(date +%s.%N)
yes yes | timeout 300 redis-cli --cluster create $NODES --cluster-replicas 1 --cluster-yes > $OUT/create.log 2>&1; echo "create rc=$?"
NODES="$NODES" $PY $H/cltool.py wait-ok > $OUT/create-ok.txt; echo "cluster ok after $(cat $OUT/create-ok.txt) s"
redis-cli -h $C1 -p 7001 CLUSTER NODES > $OUT/nodes-start.txt
# anti-affinity: each replica on another VM than its master
awk '{split($2,a,":"); print $1, a[1], $3, $4}' $OUT/nodes-start.txt > $OUT/aff.txt
python3 - $OUT/aff.txt <<'EOF'
import sys
rows=[l.split() for l in open(sys.argv[1])]
ip={r[0]:r[1] for r in rows}
bad=[r for r in rows if 'slave' in r[2] and ip.get(r[3])==r[1]]
print("replicas on their master's VM:", len(bad))
EOF
for hp in $NODES; do echo "$hp $(redis-cli -h ${hp%:*} -p ${hp#*:} CLUSTER INFO | tr -d '\r' | grep -E 'cluster_state|cluster_slots_assigned|cluster_slots_ok|cluster_known_nodes|cluster_size' | tr '\n' ' ')"; done > $OUT/cluster-info.txt
cat $OUT/cluster-info.txt
$PY $H/clw.py bigfill --seeds $SEEDS --big 100000 > $OUT/bigfill.txt 2>&1; tail -3 $OUT/bigfill.txt
# 35a413c0 re-test: prefill with bg.py (~200k keys spread over all slots) instead of redis-benchmark (whose keys
# sit in one slot per node and were deleted by hand in the earlier pass), as control2.sh did.
rm -f /tmp/bgstop; (sleep 120; touch /tmp/bgstop) & timeout 200 $PY $H/bg.py --seeds $SEEDS --procs 8 --sleep 0 --stop-file /tmp/bgstop > $OUT/prefill.txt 2>&1
for hp in $NODES; do echo "$hp $(redis-cli -h ${hp%:*} -p ${hp#*:} ROLE | head -1) $(redis-cli -h ${hp%:*} -p ${hp#*:} DBSIZE)"; done | tee -a $OUT/prefill.txt
check_all p1
echo P1-DONE
