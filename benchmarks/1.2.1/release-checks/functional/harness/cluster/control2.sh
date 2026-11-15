#!/usr/bin/env bash
# control2.sh: the control on the 1.2.0 release binary. 1.2.0 refuses to start on the 878746e4 data (it holds non-UTF-8
# keys and the empty key: the documented "going back to 1.2.0" limit), so: a fresh 3+3 cluster on 1.2.0 (same layout,
# same directives), ~200k keys from bg.py, then the same p6 rounds.
cd ~; . h/lib.sh
OUT=$R/control; mkdir -p $OUT; : > $OUT/events
DIR="cluster-enabled yes;cluster-node-timeout 5000;cluster-config-file nodes.conf;threads 4;appendfsync everysec;update-check no;enable-debug-command yes"
for h in $C1 $C2 $C3; do s $h 'bash node.sh wipeall'; s $h 'cd /opt/kv && ./kividb --version && sha256sum kividb | cut -c1-16'; s $h "bash node.sh start kividb 7001 '$DIR'; bash node.sh start kividb 7002 '$DIR'"; done
yes yes | timeout 300 redis-cli --cluster create $NODES --cluster-replicas 1 --cluster-yes > $OUT/create.log 2>&1; echo "create rc=$?"
NODES="$NODES" timeout 300 $PY $H/cltool.py wait-ok; echo " ok"
redis-cli -h $C1 -p 7001 CLUSTER NODES | awk '{print $2, $3, $4}'
rm -f /tmp/bgstop; (sleep 120; touch /tmp/bgstop) & timeout 200 $PY $H/bg.py --seeds $SEEDS --procs 8 --sleep 0 --stop-file /tmp/bgstop > /dev/null 2>&1
for hp in $NODES; do echo "$hp $(redis-cli -h ${hp%:*} -p ${hp#*:} INFO server | tr -d '\r' | grep -E '^kividb_version' ) $(redis-cli -h ${hp%:*} -p ${hp#*:} ROLE | head -1) $(redis-cli -h ${hp%:*} -p ${hp#*:} DBSIZE)"; done
for i in $(seq 1 60); do redis-cli --cluster check $C1:7001 2>&1 | grep -q 'All nodes agree' && break; sleep 2; done
TAG=c120 DELAYS="${CDELAYS:-0 0.05 0.1 0.15 0.2 0.25}" bash h/p6_fo_asm.sh
echo CONTROL-DONE
