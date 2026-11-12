#!/usr/bin/env bash
# p3_failover.sh: check 3, each scenario its own clw run under load:
#  f1 kill -9 of a master -> replica promoted by the cluster; restart the killed node -> replica again, resynced
#  f2 CLUSTER FAILOVER (manual) on a replica        f3 CLUSTER FAILOVER FORCE on a replica
#  f4 CLUSTER FAILOVER on the source's replica while a CLUSTER MIGRATION IMPORT from that source is in progress
#  f5 kill -9 of the source while a CLUSTER MIGRATION IMPORT from it is in progress (restart it afterwards)
#  f6 whole-VM loss: kill -9 both nodes on one VM (a master and another master's replica), restart both
set -u
. $HOME/h/lib.sh
after() {
  sleep 15
  stop_load
  NODES="$NODES" timeout 700 $PY $H/cltool.py wait-ok > $OUT/ok.txt; echo "cluster ok after $(cat $OUT/ok.txt)"
  check_all after
  $PY $H/clw.py bigcheck --seeds $SEEDS > $OUT/bigcheck.txt 2>&1; tail -1 $OUT/bigcheck.txt
  redis-cli -h $C1 -p 7001 CLUSTER NODES > $OUT/nodes.txt
}
waitrep() { # hp: wait replica, record
  w=$(timeout 700 $PY $H/cltool.py wait-replica $1); ev "rejoined $1 as replica, link up after $w s"
}
asm_bg() { # dst ranges... -> background, writes $OUT/asm.jsonl
  ev "asm-start dst $1 ranges ${*:2}"
  ( timeout 1000 $PY $H/mig.py asm "$@" >> $OUT/asm.jsonl; echo "$(date +%s.%N) asm-end" >> $OUT/events ) &
  ASMP=$!
}
wait_inprog() { # dst: until a task in_progress (max 10 s)
  for _ in $(seq 1 100); do redis-cli -h ${1%:*} -p ${1#*:} CLUSTER MIGRATION STATUS ALL 2>/dev/null | grep -q in_progress && return 0; sleep 0.1; done; return 1
}
sc=${ONLY:-"f1 f2 f3 f4 f5 f6"}
for f in $sc; do
case $f in
f1)
  start_load $R/p3-f1${TAG:-}
  read M MID REP REPID <<< "$($PY $H/cltool.py shard 3000)"
  ev "kill9 master $M (replica $REP)"
  nodecmd $M kill9
  p=$(timeout 300 $PY $H/cltool.py wait-master $REP); ev "promoted $REP after $p s"
  sleep 20
  ev "restart $M"; nodecmd $M restart; waitrep $M
  after;;
f2|f3)
  start_load $R/p3-$f${TAG:-}
  [ $f = f2 ] && sl=8000 || sl=13000
  read M MID REP REPID <<< "$($PY $H/cltool.py shard $sl)"
  opt=""; [ $f = f3 ] && opt=FORCE
  ev "failover $opt on $REP (master $M)"
  redis-cli -h ${REP%:*} -p ${REP#*:} CLUSTER FAILOVER $opt
  p=$(timeout 300 $PY $H/cltool.py wait-master $REP); ev "promoted $REP after $p s"
  waitrep $M
  after;;
f4|f5)
  start_load $R/p3-$f${TAG:-}
  [ $f = f4 ] && sl=500 || sl=6000
  read S SID SREP SREPID <<< "$($PY $H/cltool.py shard $sl)"
  read D _ <<< "$($PY $H/cltool.py shard $(( (sl + 5461) % 16384 )))"
  # the source's whole range from its first slot, up to 3000 slots
  rng=$(redis-cli -h ${S%:*} -p ${S#*:} CLUSTER NODES | awk '/myself/{for(i=9;i<=NF;i++) if($i!~/^\[/){print $i}}' | head -1)
  a=${rng%-*}; b=${rng#*-}; [ $((b - a)) -gt 2999 ] && b=$((a + 2999))
  asm_bg $D $a $b
  wait_inprog $D && ev "asm in_progress seen"
  sleep 0.3
  if [ $f = f4 ]; then
    ev "failover on $SREP (source $S) during asm"
    redis-cli -h ${SREP%:*} -p ${SREP#*:} CLUSTER FAILOVER
  else
    ev "kill9 source $S during asm (replica $SREP)"
    nodecmd $S kill9
  fi
  p=$(timeout 300 $PY $H/cltool.py wait-master $SREP); ev "promoted $SREP after $p s"
  wait $ASMP; tail -1 $OUT/asm.jsonl
  redis-cli -h ${D%:*} -p ${D#*:} CLUSTER MIGRATION STATUS ALL > $OUT/asm-status-dst.txt 2>&1
  redis-cli -h ${SREP%:*} -p ${SREP#*:} CLUSTER MIGRATION STATUS ALL > $OUT/asm-status-newsrc.txt 2>&1
  if [ $f = f5 ]; then sleep 10; ev "restart $S"; nodecmd $S restart; waitrep $S; else waitrep $S; fi
  # if the import did not complete, run it again to the end (from whoever owns the slots now)
  if ! tail -1 $OUT/asm.jsonl | grep -q '"completed"'; then
    sleep 5; ev "asm retry"; timeout 1000 $PY $H/mig.py asm $D $a $b | tee -a $OUT/asm.jsonl; ev "asm retry end"
  fi
  after;;
f6)
  start_load $R/p3-f6${TAG:-}
  REP=""
  for sl in 3000 8000 13000; do read M MID R2 R2ID <<< "$($PY $H/cltool.py shard $sl)"; [ ${M%:*} = $C2 ] && REP=$R2 && break; done
  ev "vm-loss $C2: kill9 7001 and 7002 (master there: $M, its replica $REP)"
  s $C2 "bash node.sh kill9 7001; bash node.sh kill9 7002"
  p=$(timeout 300 $PY $H/cltool.py wait-master $REP); ev "promoted $REP after $p s"
  sleep 15
  ev "restart $C2 nodes"; s $C2 "bash node.sh restart 7001; bash node.sh restart 7002"
  waitrep $C2:7001; waitrep $C2:7002
  after;;
esac
done
echo P3-DONE
