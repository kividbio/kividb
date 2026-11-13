#!/usr/bin/env bash
# p2a_reshard.sh: check 2a. Three rounds of `redis-cli --cluster reshard` of 1200 slots (m1->m2, m2->m3, m3->m1; the
# lowest slots of the source, i.e. 0-1199 each time, which hold the empty key and hot slots 0/100/1000/1199 with a
# big list + big hash in slot 100), each under load, each its own clw run; after each: verify, check, topo, bigcheck.
set -u
. $HOME/h/lib.sh
SLOTS=${SLOTS:-1200}
read M1 M1ID _ <<< "$($PY $H/cltool.py shard 3000)"
read M2 M2ID _ <<< "$($PY $H/cltool.py shard 8000)"
read M3 M3ID _ <<< "$($PY $H/cltool.py shard 13000)"
echo "masters $M1 $M2 $M3"
for rs in ${ROUNDS:-1:1200 2:1200 3:1200}; do
  r=${rs%:*}; SLOTS=${rs#*:}; tag=${TAG:-}
  case $r in 1) F=$M1ID; T=$M2ID;; 2) F=$M2ID; T=$M3ID;; 3) F=$M3ID; T=$M1ID;; esac
  start_load $R/p2a-r$r$tag
  ev "reshard-start round $r $SLOTS slots $F -> $T"
  t=$(date +%s.%N)
  timeout 3600 redis-cli --cluster reshard $C1:7001 --cluster-from $F --cluster-to $T --cluster-slots $SLOTS --cluster-yes > $OUT/reshard.log 2>&1
  rc=$?
  d=$(echo "$(date +%s.%N) - $t" | bc)
  ev "reshard-end round $r rc=$rc seconds=$d"
  grep -v -E '^Moving slot|^\.+$' $OUT/reshard.log | tail -5
  sleep 10
  stop_load
  check_all after
  $PY $H/clw.py bigcheck --seeds $SEEDS > $OUT/bigcheck.txt 2>&1; tail -1 $OUT/bigcheck.txt
done
echo P2A-DONE
