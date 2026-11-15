#!/usr/bin/env bash
# p5_churn.sh: one long clw run with 70 % of the writes on the hot slots, while slots move back to back:
#  0. CLUSTER MIGRATION IMPORT 0-2999 into the node that holds the orphaned keys of 0-2999 (finding 1): its old keys
#     must be dropped first, so the import must leave current values, not stale ones
#  1. 8 x ASM ping-pong of 0-1199 between two masters; on odd rounds a CLUSTER FAILOVER on the source's replica
#     0 / 50 / 150 / 300 ms after the import starts (a failover during the migration)
#  2. 3 x by hand: hot slots 100 (100k-element list/hash), 6000, 0 (empty key) there and back
#  3. redis-cli --cluster reshard of 300 slots and back
set -u
. $HOME/h/lib.sh
export HOTF=${HOTF:-0.7}
OUTD=$R/p5
owner() { $PY $H/mig.py owner $1; }
other() { for m in $(for sl in 3000 8000 13000; do owner $sl; done | sort -u); do [ $m != $1 ] && echo $m && return; done; }
repof() { $PY $H/cltool.py shard $1 | awk '{print $3}'; }
asm_retry() { # dst a b : ASM, retried until completed (max 5)
  for i in 1 2 3 4 5; do
    r=$(timeout 300 $PY $H/mig.py asm "$@"); echo "$r" >> $OUT/asm.jsonl
    echo "$r" | grep -q '"completed"' && return 0
    ev "asm not completed (try $i): $(echo $r | jq -c '{state,last_error}')"; sleep 3
  done; return 1
}
WARM=10 start_load $OUTD
ORPH=${ORPH:-10.82.1.13:7002}
ev "step0 asm 0-2999 into $ORPH (holds orphaned keys of 0-2999)"
asm_retry $ORPH 0 2999
redis-cli -h ${ORPH%:*} -p ${ORPH#*:} CLUSTER COUNTKEYSINSLOT 0 | sed 's/^/slot0 keys on dst after: /'
ev "step0 done"
delays=(0 0.05 0.15 0.3)
for i in 1 2 3 4 5 6 7 8; do
  S=$(owner 500); D=$(other $S)
  if [ $((i % 2)) = 1 ]; then
    SR=$(repof 500); dl=${delays[$(( (i / 2) % 4 ))]}
    ev "round $i: asm 0-1199 $S -> $D, failover on $SR after ${dl}s"
    ( timeout 300 $PY $H/mig.py asm $D 0 1199 >> $OUT/asm.jsonl ) &
    a=$!; sleep $dl
    redis-cli -h ${SR%:*} -p ${SR#*:} CLUSTER FAILOVER > /dev/null
    timeout 120 $PY $H/cltool.py wait-master $SR > /dev/null
    wait $a
    tail -1 $OUT/asm.jsonl | jq -c '{state,last_error,retries,wall_s}'
    timeout 300 $PY $H/cltool.py wait-replica $S > /dev/null
    # if the import did not complete, finish it
    tail -1 $OUT/asm.jsonl | grep -q '"completed"' || { ev "round $i: asm did not complete, again"; asm_retry $D 0 1199; }
  else
    ev "round $i: asm 0-1199 $S -> $D"
    asm_retry $D 0 1199; tail -1 $OUT/asm.jsonl | jq -c '{state,retries,wall_s}'
  fi
done
ev "step1 done"
for j in 1 2 3; do
  for sl in 100 6000 0; do
    S=$(owner $sl); D=$(other $S)
    timeout 300 $PY $H/mig.py hand $D $sl >> $OUT/hand.jsonl
    timeout 300 $PY $H/mig.py hand $S $sl >> $OUT/hand.jsonl
  done
done
ev "step2 done: $(wc -l < $OUT/hand.jsonl) hand moves, retries $(jq -s 'map(.retries) | add' $OUT/hand.jsonl)"
read A AID _ <<< "$($PY $H/cltool.py shard 8000)"
read B BID _ <<< "$($PY $H/cltool.py shard 13000)"
for x in "$AID $BID" "$BID $AID"; do
  set -- $x
  ev "reshard 300 slots $1 -> $2"
  t=$(date +%s.%N)
  timeout 1800 redis-cli --cluster reshard $C1:7001 --cluster-from $1 --cluster-to $2 --cluster-slots 300 --cluster-yes > $OUT/reshard-$1.log 2>&1
  ev "reshard rc=$? seconds=$(echo "$(date +%s.%N) - $t" | bc)"
done
sleep 10
stop_load
NODES="$NODES" timeout 700 $PY $H/cltool.py wait-ok > $OUT/ok.txt
check_all after
$PY $H/clw.py bigcheck --seeds $SEEDS > $OUT/bigcheck.txt 2>&1; tail -1 $OUT/bigcheck.txt
echo P5-DONE
