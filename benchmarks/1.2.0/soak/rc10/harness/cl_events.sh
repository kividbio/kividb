#!/usr/bin/env bash
# cl_events.sh <out>: the rc10 cluster soak's events, at fixed minutes after soak.start (env SCHED overrides the list).
# Shards are named by a slot no event moves (3000, 8000, 13000). Lines "unix_ts name ..." go to <out>/events.
#   failover <slot>                 CLUSTER FAILOVER on the shard's replica
#   kill9 <slot>                    kill -9 of the shard's master; restarted 20 s later, rejoins as a replica
#   import <from> <to> <lo> <hi>    CLUSTER MIGRATION IMPORT lo hi on the destination master, STATUS polled
#   reshard <from> <to> <n>         ONE plain `redis-cli --cluster reshard` of n slots (no --cluster-replace, no
#                                   slotfix.py, no --cluster fix): it must exit 0 and leave no slot open. owcheck.py
#                                   overwrites keys in the moving slots ("hot keys") while it runs. INFO commandstats
#                                   and SLOWLOG of source and destination before/after (MIGRATE / SETSLOT times, #310)
#   sigterm <slot>                  SIGTERM of the shard's replica under load: it must exit within 30 s with its final
#                                   save ("Writing final snapshot" + "Goodbye."), #308. Restarted at once; whether it
#                                   resyncs partially is recorded
# After every event that makes a node sync again, the node's log after the event is read for "Partial resync" /
# "Full resync" lines, and INFO stats sync_* of every node is appended to <out>/syncstats.txt.
# failover, kill9 and imp are the rc8 soak's functions (markers added); reshard and sigterm are new for rc10.
set -u
OUT=$1
. $HOME/clhosts.env
export NODES="$C1:7001 $C2:7001 $C3:7001 $C1:7002 $C2:7002 $C3:7002"
PY=$HOME/venv/bin/python
T=$HOME/cltool.py
EV=$OUT/events
SCHED=${SCHED:-"10 failover 3000;16 sigterm 8000;22 kill9 8000;30 reshard 3000 8000 128;42 sigterm 13000;50 import 13000 3000 10923 11422;56 sigterm 3000;62 kill9 3000;68 reshard 13000 8000 128;78 failover 13000;85 import 8000 13000 5461 5960;92 reshard 8000 3000 128;104 sigterm 8000;110 kill9 13000"}
s() { for i in 1 2 3 4 5 6; do ssh "$@"; rc=$?; [ $rc != 255 ] && return $rc; sleep 3; done; return 255; }
ev() { echo "$(date +%s.%N) $*" >> $EV; echo "$(date -u +%T) $*"; }
el() { awk -v a=$1 -v b=$(date +%s.%N) 'BEGIN{printf "%.2f", b-a}'; }
until [ -f $OUT/soak.start ]; do sleep 1; done
T0=$(cut -d. -f1 $OUT/soak.start)
at() { while [ $(date +%s) -lt $((T0 + $1 * 60)) ]; do sleep 1; done; }
dbsize() { timeout 3 redis-cli -h ${1%:*} -p ${1#*:} DBSIZE; }
syncsnap() { # label
  for hp in $NODES; do
    echo "$(date +%s.%N) $1 $hp $(timeout 2 redis-cli -h ${hp%:*} -p ${hp#*:} INFO stats 2>/dev/null | tr -d '\r' | awk -F: '$1~/^sync_(full|partial_ok|partial_err)$/{printf "%s=%s ",$1,$2}')" >> $OUT/syncstats.txt
  done
}
mark() { # host:port text: a line in the node's log, to find what it logged after
  s ${1%:*} "echo '=== $2 '\$(date -u +%FT%T.%3NZ) >> /data/${1#*:}/server.log"
}
resync_after() { # host:port marker: the node's sync lines after the last marker line
  s ${1%:*} "tac /data/${1#*:}/server.log | sed '/=== $2/q' | tac | grep -E 'Partial resync|Full resync|FULL RESYNC|Full sync loaded|NOT syncing' | cut -c1-220" > $OUT/resync-tmp.txt
  echo "partial=$(grep -c 'Partial resync' $OUT/resync-tmp.txt) full=$(grep -c -E 'Full resync from' $OUT/resync-tmp.txt) lines: $(tr '\n' '|' < $OUT/resync-tmp.txt | cut -c1-500)"
}
failover() { # slot
  read m mid r rid <<< "$($PY $T shard $1)"
  fo=$(( ${fo:-0} + 1 ))
  ev "failover-start slot=$1 master=$m replica=$r"
  mark $m "failover-$fo old master"
  ev "failover-reply $(redis-cli -h ${r%:*} -p ${r#*:} CLUSTER FAILOVER)"
  ev "failover-promoted $r after $($PY $T wait-master $r) s"
  ev "failover-old-master-replica $m after $($PY $T wait-replica $m) s"
  ev "failover-end cluster-ok after $($PY $T wait-ok) s"
  sleep 5
  ev "failover-resync $m $(resync_after $m "failover-$fo old master")"
  syncsnap failover-$fo
}
kill9() { # slot
  read m mid r rid <<< "$($PY $T shard $1)"
  k9=$(( ${k9:-0} + 1 ))
  ev "kill9-start slot=$1 master=$m replica=$r"
  s ${m%:*} "bash node.sh kill9 ${m#*:}"
  ev "kill9-killed $m"
  ev "kill9-promoted $r after $($PY $T wait-master $r) s"
  sleep 20
  mark $m "restart after kill9-$k9"
  s ${m%:*} "bash node.sh restart ${m#*:}"
  ev "kill9-restarted $m"
  ev "kill9-rejoined $m as replica after $($PY $T wait-replica $m) s"
  ev "kill9-end cluster-ok after $($PY $T wait-ok) s"
  sleep 5
  ev "kill9-resync $m $(resync_after $m "restart after kill9-$k9")"
  syncsnap kill9-$k9
}
st=0
sigterm() { # slot
  st=$((st + 1))
  read m mid r rid <<< "$($PY $T shard $1)"
  local h=${r%:*} p=${r#*:} L=$OUT/sigterm-$st
  mkdir -p $L
  ev "sigterm-start round $st slot=$1 replica=$r master=$m: $(timeout 2 redis-cli -h $h -p $p INFO replication | tr -d '\r' | grep -E '^(master_link_status|slave_repl_offset|master_sync_in_progress):' | tr '\n' ' ')"
  mark $r "sigterm-$st"
  local t0=$(date +%s.%N)
  local res=$(s $h "p=\$(cat /data/$p/pid); kill -TERM \$p; for i in \$(seq 1 600); do kill -0 \$p 2>/dev/null || { echo exited; exit 0; }; sleep 0.05; done; echo HUNG \$p")
  local ex=$(el $t0)
  s $h "tac /data/$p/server.log | sed '/=== sigterm-$st/q' | tac" > $L/log-at-stop.txt
  s $h "ls -l --time-style=+%s /data/$p/" > $L/datadir.txt
  local saved=$(grep -c -i -E 'writing final snapshot' $L/log-at-stop.txt) bye=$(grep -c 'Goodbye\.' $L/log-at-stop.txt) err=$(grep -c '\[ERROR\]' $L/log-at-stop.txt) skip=$(grep -c -i 'skip' $L/log-at-stop.txt)
  if [ "${res%% *}" = HUNG ]; then
    s $h "sudo gdb -p ${res#HUNG } -batch -ex 'thread apply all bt 25' 2>&1" > $L/stacks.txt
    s $h "kill -9 ${res#HUNG }; sleep 1"
  fi
  ev "sigterm-exit round $st $r result=${res%% *} after $ex s final-save=$([ "$saved" -ge 1 ] && [ "$bye" -ge 1 ] && echo yes || echo NO) (writing-final-snapshot=$saved goodbye=$bye error-lines=$err skip-lines=$skip)"
  # restarted at once, as a rolling restart would: at the soak's load a shard streams ~8 MB/s, so the 64 MB backlog
  # covers ~8 s of absence (the smoke run's 5 s pause before the restart made it 8 s, and the replica full-synced)
  mark $r "restart after sigterm-$st"
  s $h "bash node.sh restart $p"
  ev "sigterm-restarted $r"
  ev "sigterm-rejoined $r as replica after $($PY $T wait-replica $r) s"
  ev "sigterm-end cluster-ok after $($PY $T wait-ok) s"
  sleep 5
  ev "sigterm-resync $r $(resync_after $r "restart after sigterm-$st")"
  syncsnap sigterm-$st
}
n=0
cmdstats() { # host:port out
  timeout 5 redis-cli -h ${1%:*} -p ${1#*:} INFO commandstats 2>/dev/null | tr -d '\r' | grep -E '^cmdstat_(migrate|cluster|asking|restore)' > $2
}
reshard() { # from-slot to-slot n: one plain redis-cli run, must exit 0
  n=$((n + 1))
  read m mid r rid <<< "$($PY $T shard $1)"
  read m2 mid2 r2 rid2 <<< "$($PY $T shard $2)"
  local L=$OUT/reshard-$n
  mkdir -p $L
  local hot=$($PY $HOME/slotfix.py first $m $3)
  echo "$hot" > $OUT/hot-slots
  echo "$m $mid $m2 $mid2 $hot" > $L/plan.txt
  sleep 15
  for x in $m $m2; do timeout 3 redis-cli -h ${x%:*} -p ${x#*:} SLOWLOG RESET > /dev/null; done
  cmdstats $m $L/cmdstats-src-before.txt; cmdstats $m2 $L/cmdstats-dst-before.txt
  ev "reshard-start round $n src-shard=$1 dst-shard=$2: $3 slots ($(echo $hot | awk '{print $1"-"$NF}')) from $m ($mid, $(dbsize $m) keys) to $m2 ($mid2) with plain redis-cli"
  local t=$(date +%s.%N)
  timeout 3000 redis-cli --cluster reshard $m --cluster-from $mid --cluster-to $mid2 --cluster-slots $3 --cluster-yes > $L/reshard.log 2>&1
  local rc=$?
  ev "reshard-end round $n exit=$rc after $(el $t) s: slots begun=$(grep -c '^Moving slot' $L/reshard.log), TRYAGAIN=$(grep -c TRYAGAIN $L/reshard.log), ERR=$(grep -c -E 'ERR|failed' $L/reshard.log): $(grep -E 'TRYAGAIN|ERR|failed' $L/reshard.log | tail -1 | cut -c1-200)"
  cmdstats $m $L/cmdstats-src-after.txt; cmdstats $m2 $L/cmdstats-dst-after.txt
  $PY $HOME/slowdump.py $m $L/slowlog-src.json; $PY $HOME/slowdump.py $m2 $L/slowlog-dst.json
  local open=$($PY $HOME/slotfix.py open $m | tr '\n' ' ')
  redis-cli --cluster check $m > $L/check.txt 2>&1
  ev "reshard-check round $n: open-on-source=[${open}] open-slots-lines=$(grep -c 'slots are open' $L/check.txt) covered=$(grep -c 'All 16384 slots covered' $L/check.txt) err-lines=$(grep -c '^\[ERR\]' $L/check.txt) moved-slots=$(( $(echo $hot | wc -w) - $(comm -12 <(echo $hot | tr ' ' '\n' | sort) <($PY $HOME/slotfix.py first $m 16384 | tr ' ' '\n' | sort) | wc -l) ))"
  if [ -n "$open" ]; then
    # only so that the soak can go on: the round has already failed
    ev "reshard-FAILED round $n: slot left open, closed by slotfix.py"
    while read -r sl dest; do
      [ -z "$sl" ] && continue
      ev "reshard-slotfix slot $sl: $($PY $HOME/slotfix.py repair $m $m2 $sl | cut -c1-600)"
    done <<< "$($PY $HOME/slotfix.py open $m)"
  fi
  sleep 10
  echo > $OUT/hot-slots
  ev "reshard-done round $n cluster-ok after $($PY $T wait-ok) s"
}
imp() { # from-slot to-slot lo hi
  im=$(( ${im:-0} + 1 ))
  read sm sid sr srid <<< "$($PY $T shard $1)"
  read dm did dr drid <<< "$($PY $T shard $2)"
  echo "ts,ping_source_ms,ping_dest_ms" > $OUT/import-probe-$im.csv
  ( while :; do a=$(date +%s%N); redis-cli -h ${sm%:*} -p ${sm#*:} PING > /dev/null; b=$(date +%s%N); redis-cli -h ${dm%:*} -p ${dm#*:} PING > /dev/null; c=$(date +%s%N)
    echo "$(date +%s.%N),$(( (b - a) / 1000 ))e-3,$(( (c - b) / 1000 ))e-3" >> $OUT/import-probe-$im.csv; sleep 0.5; done ) &
  local P=$!
  ev "import-start round $im CLUSTER MIGRATION IMPORT $3 $4 on $dm ($did, $(dbsize $dm) keys), source $sm ($sid, $(dbsize $sm) keys)"
  local id=$(redis-cli -h ${dm%:*} -p ${dm#*:} CLUSTER MIGRATION IMPORT $3 $4)
  ev "import-id $id"
  local t=$(date +%s.%N) st=""
  for _ in $(seq 1 3600); do
    st=$(redis-cli -h ${dm%:*} -p ${dm#*:} CLUSTER MIGRATION STATUS ID "$id" | tr '\n' ' ')
    echo "$(date +%s.%N) $st" >> $OUT/import-status-$im.log
    echo "$st" | grep -q -i -E 'state (completed|done|finished|failed|canceled|cancelled)' && break
    sleep 0.5
  done
  ev "import-done round $im in $(el $t) s: $(echo "$st" | cut -c1-320)"
  ev "import-keys source $(dbsize $sm) dest $(dbsize $dm)"
  ev "import-end cluster-ok after $($PY $T wait-ok) s"
  sleep 10; kill $P
  redis-cli --cluster check $dm > $OUT/check-after-import-$im.txt 2>&1
}
syncsnap start
IFS=';' read -ra EVS <<< "$SCHED"
for e in "${EVS[@]}"; do
  set -- $e
  at $1; shift
  case $1 in
    failover) failover $2 ;;
    kill9) kill9 $2 ;;
    reshard) reshard $2 $3 $4 ;;
    import) imp $2 $3 $4 $5 ;;
    sigterm) sigterm $2 ;;
  esac
  redis-cli -h $C1 -p 7001 CLUSTER NODES > $OUT/nodes-after-$(echo $e | tr ' ' '-').txt 2>&1
done
syncsnap end
ev "events-done"
