#!/usr/bin/env bash
# cl_events.sh <out>: the cluster soak's events, at fixed minutes after soak.start (env SCHED overrides the list).
# Shards are named by a slot no event moves (3000, 8000, 13000). Lines "unix_ts name ..." go to <out>/events.
#   failover <slot>                 CLUSTER FAILOVER on the shard's replica
#   kill9 <slot>                    kill -9 of the shard's master; restarted 20 s later, rejoins as a replica
#   reshard <from> <to> <n>         redis-cli --cluster reshard of n slots, run to completion
#   import <from> <to> <lo> <hi>    CLUSTER MIGRATION IMPORT lo hi on the destination master, STATUS polled
# Functions are the rc6 soak's cl_events.sh and imp.sh, without bc.
set -u
OUT=$1
. $HOME/clhosts.env
export NODES="$C1:7001 $C2:7001 $C3:7001 $C1:7002 $C2:7002 $C3:7002"
PY=$HOME/venv/bin/python
T=$HOME/cltool.py
EV=$OUT/events
SCHED=${SCHED:-"10 failover 3000;22 kill9 8000;35 reshard 3000 8000 10;50 import 13000 3000 10923 11422;62 kill9 3000;75 failover 13000;85 import 8000 13000 5461 5960;95 reshard 13000 3000 5;110 kill9 13000"}
s() { for i in 1 2 3 4 5 6; do ssh "$@"; rc=$?; [ $rc != 255 ] && return $rc; sleep 3; done; return 255; }
ev() { echo "$(date +%s.%N) $*" >> $EV; echo "$(date -u +%T) $*"; }
el() { awk -v a=$1 -v b=$(date +%s.%N) 'BEGIN{printf "%.2f", b-a}'; }
until [ -f $OUT/soak.start ]; do sleep 1; done
T0=$(cut -d. -f1 $OUT/soak.start)
at() { while [ $(date +%s) -lt $((T0 + $1 * 60)) ]; do sleep 1; done; }
dbsize() { timeout 3 redis-cli -h ${1%:*} -p ${1#*:} DBSIZE; }
failover() { # slot
  read m mid r rid <<< "$($PY $T shard $1)"
  ev "failover-start slot=$1 master=$m replica=$r"
  ev "failover-reply $(redis-cli -h ${r%:*} -p ${r#*:} CLUSTER FAILOVER)"
  ev "failover-promoted $r after $($PY $T wait-master $r) s"
  ev "failover-old-master-replica $m after $($PY $T wait-replica $m) s"
  ev "failover-end cluster-ok after $($PY $T wait-ok) s"
}
kill9() { # slot
  read m mid r rid <<< "$($PY $T shard $1)"
  ev "kill9-start slot=$1 master=$m replica=$r"
  s ${m%:*} "bash node.sh kill9 ${m#*:}"
  ev "kill9-killed $m"
  ev "kill9-promoted $r after $($PY $T wait-master $r) s"
  sleep 20
  s ${m%:*} "echo '=== restart after kill9 '\$(date -u +%FT%TZ) >> /data/${m#*:}/server.log; bash node.sh restart ${m#*:}"
  ev "kill9-restarted $m"
  ev "kill9-rejoined $m as replica after $($PY $T wait-replica $m) s"
  ev "kill9-end cluster-ok after $($PY $T wait-ok) s"
}
n=0
reshard() { # from-slot to-slot n
  n=$((n + 1))
  read m mid r rid <<< "$($PY $T shard $1)"
  read m2 mid2 r2 rid2 <<< "$($PY $T shard $2)"
  ev "reshard-start $3 slots from $m ($mid, $(dbsize $m) keys) to $m2 ($mid2) with redis-cli"
  local t=$(date +%s.%N) before=$($PY $HOME/slotfix.py nslots $m) moved=0 try=0 rc
  # redis-cli stops at the first TRYAGAIN (a key written while MIGRATE carried it, #274) and leaves the slot open;
  # --cluster fix cannot close it (CLUSTER DELSLOTS, rc6 finding C), so the open slot is finished by slotfix.py and
  # redis-cli is run again for the slots still to move.
  while [ $moved -lt $3 ] && [ $try -lt 30 ]; do
    try=$((try + 1))
    timeout 1500 redis-cli --cluster reshard $m --cluster-from $mid --cluster-to $mid2 --cluster-slots $(($3 - moved)) --cluster-yes --cluster-replace >> $OUT/reshard-$n.log 2>&1
    rc=$?
    ev "reshard-attempt $try exit=$rc after $(el $t) s: $(grep -E 'TRYAGAIN|ERR|failed' $OUT/reshard-$n.log | tail -1 | cut -c1-200)"
    while read -r sl dest; do
      [ -z "$sl" ] && continue
      ev "reshard-repair slot $sl: $($PY $HOME/slotfix.py repair $m $m2 $sl | cut -c1-600)"
    done <<< "$($PY $HOME/slotfix.py open $m)"
    moved=$((before - $($PY $HOME/slotfix.py nslots $m)))
    [ $rc = 0 ] && break
  done
  ev "reshard-done in $(el $t) s: $moved slots moved in $try redis-cli runs, slots begun=$(grep -c '^Moving slot' $OUT/reshard-$n.log)"
  ev "reshard-end cluster-ok after $($PY $T wait-ok) s"
  redis-cli --cluster check $m > $OUT/check-after-reshard-$n.txt 2>&1
}
imp() { # from-slot to-slot lo hi
  n=$((n + 1))
  read sm sid sr srid <<< "$($PY $T shard $1)"
  read dm did dr drid <<< "$($PY $T shard $2)"
  echo "ts,ping_source_ms,ping_dest_ms" > $OUT/import-probe-$n.csv
  ( while :; do a=$(date +%s%N); redis-cli -h ${sm%:*} -p ${sm#*:} PING > /dev/null; b=$(date +%s%N); redis-cli -h ${dm%:*} -p ${dm#*:} PING > /dev/null; c=$(date +%s%N)
    echo "$(date +%s.%N),$(( (b - a) / 1000 ))e-3,$(( (c - b) / 1000 ))e-3" >> $OUT/import-probe-$n.csv; sleep 0.5; done ) &
  local P=$!
  ev "import-start CLUSTER MIGRATION IMPORT $3 $4 on $dm ($did, $(dbsize $dm) keys), source $sm ($sid, $(dbsize $sm) keys)"
  local id=$(redis-cli -h ${dm%:*} -p ${dm#*:} CLUSTER MIGRATION IMPORT $3 $4)
  ev "import-id $id"
  local t=$(date +%s.%N) st=""
  for _ in $(seq 1 3600); do
    st=$(redis-cli -h ${dm%:*} -p ${dm#*:} CLUSTER MIGRATION STATUS ID "$id" | tr '\n' ' ')
    echo "$(date +%s.%N) $st" >> $OUT/import-status-$n.log
    echo "$st" | grep -q -i -E 'state (completed|done|finished|failed|canceled|cancelled)' && break
    sleep 0.5
  done
  ev "import-done in $(el $t) s: $(echo "$st" | cut -c1-320)"
  ev "import-keys source $(dbsize $sm) dest $(dbsize $dm)"
  ev "import-end cluster-ok after $($PY $T wait-ok) s"
  sleep 10; kill $P
  redis-cli --cluster check $dm > $OUT/check-after-import-$n.txt 2>&1
}
IFS=';' read -ra EVS <<< "$SCHED"
for e in "${EVS[@]}"; do
  set -- $e
  at $1; shift
  case $1 in
    failover) failover $2 ;;
    kill9) kill9 $2 ;;
    reshard) reshard $2 $3 $4 ;;
    import) imp $2 $3 $4 $5 ;;
  esac
  redis-cli -h $C1 -p 7001 CLUSTER NODES > $OUT/nodes-after-$(echo $e | tr ' ' '-').txt 2>&1
done
ev "events-done"
