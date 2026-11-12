#!/usr/bin/env bash
# p2bc_move.sh: checks 2b (CLUSTER MIGRATION IMPORT, 4 rounds, multi-range, every hot slot) and 2c (by-hand
# SETSLOT IMPORTING/MIGRATING + MIGRATE of the slots holding binary keys and the 100k-element collections, there and
# back). Each round under load, its own clw run; after each: verify, --cluster check, topo, bigcheck.
set -u
. $HOME/h/lib.sh
read M1 M1ID _ <<< "$($PY $H/cltool.py shard 3000)"
read M2 M2ID _ <<< "$($PY $H/cltool.py shard 8000)"
read M3 M3ID _ <<< "$($PY $H/cltool.py shard 13000)"
echo "masters $M1 $M2 $M3"
after() {
  sleep 10
  stop_load
  check_all after
  $PY $H/clw.py bigcheck --seeds $SEEDS > $OUT/bigcheck.txt 2>&1; tail -1 $OUT/bigcheck.txt
}
asm() { # dst ranges...
  ev "asm-start dst $1 ranges ${*:2}"
  timeout 1000 $PY $H/mig.py asm "$@" | tee -a $OUT/asm.jsonl
  ev "asm-end"
}
if [ "${ONLY:-}" != c ]; then
start_load $R/p2b-r1; asm $M2 0 1199; after
start_load $R/p2b-r2; asm $M3 0 1199 5461 6999; after
start_load $R/p2b-r3; asm $M1 0 1199 5461 6999 10923 12499; after
start_load $R/p2b-r4; asm $M2 5461 6999; asm $M3 10923 12499; after
fi
HS="0 100 6000 12000 16383"
start_load $R/p2c-r1
ev "hand-start there"
for sl in $HS; do
  o=$($PY $H/mig.py owner $sl)
  case $o in $M1) d=$M2;; $M2) d=$M3;; $M3) d=$M1;; esac
  timeout 900 $PY $H/mig.py hand $d $sl | tee -a $OUT/hand.jsonl
done
ev "hand-end there"
after
start_load $R/p2c-r2
ev "hand-start back"
for sl in $HS; do
  case $sl in 0|100) d=$M1;; 6000) d=$M2;; *) d=$M3;; esac
  timeout 900 $PY $H/mig.py hand $d $sl | tee -a $OUT/hand.jsonl
done
ev "hand-end back"
after
echo P2BC-DONE
