#!/usr/bin/env bash
# control.sh: the same 3-VM ASM + failover rounds on the 1.2.0 release binary (expected to lose writes).
cd ~; . h/lib.sh
OUT=$R/control; mkdir -p $OUT; : > $OUT/events
for h in $C1 $C2 $C3; do s $h 'bash node.sh term 7001; bash node.sh term 7002; cp /opt/kv/kividb /opt/kv/kividb-878746e4; cp /opt/v120/kividb-bin /opt/kv/kividb; /opt/kv/kividb --version; bash node.sh restart 7001; bash node.sh restart 7002'; done
NODES="$NODES" timeout 300 $PY $H/cltool.py wait-ok; echo " ok"
for hp in $NODES; do echo "$hp $(redis-cli -h ${hp%:*} -p ${hp#*:} INFO server | tr -d '\r' | grep -E '^kividb_version|^redis_version' | tr '\n' ' ') $(redis-cli -h ${hp%:*} -p ${hp#*:} ROLE | head -1)"; done
for i in $(seq 1 60); do redis-cli --cluster check $C1:7001 2>&1 | grep -q 'All nodes agree' && break; sleep 2; done
TAG=c120 DELAYS="${CDELAYS:-0 0.05 0.1 0.15 0.2 0.25}" bash h/p6_fo_asm.sh
echo CONTROL-DONE
