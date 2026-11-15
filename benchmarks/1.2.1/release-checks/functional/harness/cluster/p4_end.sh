#!/usr/bin/env bash
# p4_end.sh: check 4. Quiesced: each replica vs its master key by key (DEBUG DIGEST-VALUE), CLUSTER SLOTS/SHARDS the
# same on all six, --cluster check, every acked write of every run re-verified at the end, logs for panics/errors.
set -u
. $HOME/h/lib.sh
OUT=$R/p4; rm -rf $OUT; mkdir -p $OUT; : > $OUT/events
sleep 5
check_all end
timeout 1800 $PY $H/clcheck.py replicas > $OUT/replicas.json 2>&1; echo "replicas rc=$?"
jq -c '.pairs[] | {master,replica,offsets_equal,master_keys,replica_keys,only_master,only_replica,digest_differences}' $OUT/replicas.json
for hp in $NODES; do echo "$hp $(redis-cli -h ${hp%:*} -p ${hp#*:} INFO stats | tr -d '\r' | awk -F: '$1~/^sync_(full|partial_ok|partial_err)$/{printf "%s=%s ",$1,$2}')"; done > $OUT/syncstats.txt
cat $OUT/syncstats.txt
# every earlier run re-verified against the final cluster
for d in $R/p2* $R/p3* $R/p6*-r*; do
  [ -d $d ] || continue
  mkdir -p $OUT/reverify/$(basename $d)
  cp $d/w*.log $d/w*.tl.json $d/t0 $OUT/reverify/$(basename $d)/ 2>/dev/null
  echo "$(basename $d) $($PY $H/clw.py verify --allow-newer --seeds $SEEDS --out $OUT/reverify/$(basename $d) | jq -c '{acked_writes,lost,stale,wrong,list_lost,list_dup,list_disorder,list_extra,read_errors}')"
done > $OUT/reverify.txt 2>&1
cat $OUT/reverify.txt
for h in $C1 $C2 $C3; do for p in 7001 7002; do scp -q $h:/data/$p/server.log $OUT/server-$h-$p.log; done; done
grep -c -i -E 'panicked|\[ERROR\]' $OUT/server-*.log > $OUT/error-lines.txt; cat $OUT/error-lines.txt
echo P4-DONE
