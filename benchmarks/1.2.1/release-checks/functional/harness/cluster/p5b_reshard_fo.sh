#!/usr/bin/env bash
# p5b_reshard_fo.sh: under load (70 % hot): redis-cli --cluster reshard 300 slots A->B; then 300 slots B->A with a
# CLUSTER FAILOVER on B's replica 5 s into it (a failover during a key-by-key migration); then --cluster fix if a slot
# was left open, and the rest of the reshard. Verify, check, topo, bigcheck.
set -u
. $HOME/h/lib.sh
export HOTF=0.7
agree() { for _ in $(seq 1 60); do redis-cli --cluster check $C1:7001 2>&1 | grep -q 'All nodes agree' && return 0; sleep 1; done; return 1; }
read A AID _ <<< "$($PY $H/cltool.py shard 8000)"
read B BID BR BRID <<< "$($PY $H/cltool.py shard 13000)"
WARM=10 start_load $R/p5b
agree
ev "reshard 300 $A -> $B"; t=$(date +%s.%N)
timeout 1800 redis-cli --cluster reshard $C1:7001 --cluster-from $AID --cluster-to $BID --cluster-slots 300 --cluster-yes > $OUT/reshard-1.log 2>&1
ev "reshard rc=$? seconds=$(echo "$(date +%s.%N) - $t" | bc)"
agree
ev "reshard 300 $B -> $A, failover on $BR after 5 s"; t=$(date +%s.%N)
timeout 1800 redis-cli --cluster reshard $C1:7001 --cluster-from $BID --cluster-to $AID --cluster-slots 300 --cluster-yes > $OUT/reshard-2.log 2>&1 &
RS=$!
sleep 5
redis-cli -h ${BR%:*} -p ${BR#*:} CLUSTER FAILOVER
p=$(timeout 120 $PY $H/cltool.py wait-master $BR); ev "promoted $BR after $p s"
wait $RS; ev "reshard rc=$? seconds=$(echo "$(date +%s.%N) - $t" | bc), moved $(grep -c '^Moving slot' $OUT/reshard-2.log)"
grep -v -E '^Moving slot|^\.+$' $OUT/reshard-2.log | tail -4
timeout 300 $PY $H/cltool.py wait-replica $B > /dev/null
redis-cli -h $C1 -p 7001 CLUSTER NODES | grep -o '\[[^]]*\]' | sort -u | head > $OUT/open-slots.txt
ev "open slots after: $(tr '\n' ' ' < $OUT/open-slots.txt)"
if [ -s $OUT/open-slots.txt ]; then
  ev "cluster fix"; timeout 900 redis-cli --cluster fix $C1:7001 --cluster-yes > $OUT/fix.log 2>&1; ev "fix rc=$?"
  grep -E '^\[|>>>|\*\*\*' $OUT/fix.log | tail -8
fi
sleep 10
stop_load
NODES="$NODES" timeout 700 $PY $H/cltool.py wait-ok > $OUT/ok.txt
check_all after
$PY $H/clw.py bigcheck --seeds $SEEDS > $OUT/bigcheck.txt 2>&1; tail -1 $OUT/bigcheck.txt
echo P5B-DONE
