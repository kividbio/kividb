#!/usr/bin/env bash
# fexp_scen.sh <scenario> <rep> [master-engine] [replica-engine]   (client VM; engines kividb=1.2.1, kv120=1.2.0)
# Hash-field deadlines passing while a replica lags or full-syncs (#415, #531). Master on FO1, replica FO2
# (and FO3 for chain/promote). fexp_bulk.py loads 20k hashes x 20 fields with deadlines 3-25 s out and writes
# at random fields for 40 s while they expire; then, after every deadline has passed, master and replica are
# compared exactly (values and HPEXPIRETIME).
#   lag_stop   SIGSTOP the replica at t=6 s for 8 s, and at t=20 s for 6 s
#   lag_netem  1500 ms netem delay on the master's link to the replica from t=5 to t=25 s; hide checks on the
#              replica during it
#   fullsync   master also holds 3M 200-byte strings; the replica is started at t=3 s (fresh, full sync takes
#              seconds) while fields expire and writes go on; sync time recorded
#   chain      FO1 -> FO2 -> FO3 (replica of a replica); compare FO1 with both
#   promote    chain; at t=12 s kill -9 FO1, REPLICAOF NO ONE on FO2, mutator moves to FO2; FO2 must now expire
#              fields itself and send HDEL to FO3 (expired_subkeys on FO2 rises; FO3 equals FO2)
#   race       fexp_ext.py (every race mode + plain modes) across hosts
set -u
H=$(cd "$(dirname "$0")" && pwd); . "$H/hosts.env"
S=$1 REP=$2 EM=${3:-kividb} ER=${4:-kividb}
OUT=${RES:-$HOME/fexp-results}/$EM-$ER/$S/$REP; rm -rf "$OUT"; mkdir -p "$OUT"
PY=$HOME/venv/bin/python
n()  { local h=$1; shift; ssh -o StrictHostKeyChecking=no -o LogLevel=ERROR "$h" "bash node.sh $(printf '%q ' "$@")"; }
sx() { local h=$1; shift; ssh -o StrictHostKeyChecking=no -o LogLevel=ERROR "$h" "$@"; }
rc() { local hp=$1; shift; redis-cli -h "${hp%:*}" -p "${hp#*:}" "$@" 2>&1; }
info() { rc "$1" INFO "$2" | tr -d '\r' | awk -F: -v k="$3" '$1==k{print $2; exit}'; }
log() { echo "$(date +%T.%3N) $*" | tee -a "$OUT/log.txt"; }
waitlink() { for _ in $(seq 1 ${2:-600}); do [ "$(info "$1" replication master_link_status)" = up ] && return 0; sleep 0.1; done; return 1; }
waitoff() { for _ in $(seq 1 240); do a=$(info "$1" replication master_repl_offset); b=$(info "$2" replication slave_repl_offset)
  [ -n "$a" ] && [ "$a" = "$b" ] && return 0; sleep 0.5; done; return 1; }
M=$FO1:6379; R=$FO2:6379; R3=$FO3:6379
for h in $FO1 $FO2 $FO3; do n "$h" wipeall >/dev/null 2>&1; sx $h "sudo tc qdisc del dev eth0 root 2>/dev/null"; done
n "$FO1" start "$EM" 6379 "appendfsync everysec" >/dev/null
log "master $(info $M server kividb_version) expired_fields_propagated=$(info $M server kividb_expired_fields_propagated)"
if [ "$S" = fullsync ]; then
  rc $M DEBUG POPULATE 3000000 filler 200 >/dev/null; log "master filler: $(rc $M DBSIZE) keys, used_memory_human=$(info $M memory used_memory_human)"
else
  n "$FO2" start "$ER" 6379 "replicaof $FO1 6379;appendfsync everysec" >/dev/null; waitlink $R; log "replica link up"
  if [ "$S" = chain ] || [ "$S" = promote ]; then
    n "$FO3" start "$ER" 6379 "replicaof $FO2 6379;appendfsync everysec" >/dev/null; waitlink $R3 && log "sub-replica link up" || log "sub-replica link NOT up (KiviDB refuses to chain replicas)"
  fi
fi
if [ "$S" = race ]; then
  $PY "$H/fexp_ext.py" $M $R --n ${N:-300} --out "$OUT/fexp_ext.json" | tee -a "$OUT/log.txt"
else
  T0=$(date +%s.%N)
  $PY "$H/fexp_bulk.py" load $M | tee -a "$OUT/log.txt"
  MT=$M
  $PY "$H/fexp_bulk.py" mutate $MT --seconds 40 > "$OUT/mutate.txt" 2>&1 & MU=$!
  el() { awk -v a="$T0" -v b="$(date +%s.%N)" 'BEGIN{printf "%.1f", b-a}'; }
  case $S in
  lag_stop)
    sleep 6; n "$FO2" stop 6379; log "SIGSTOP replica (t=$(el))"; sleep 8; n "$FO2" cont 6379; log "SIGCONT replica"
    sleep 6; n "$FO2" stop 6379; log "SIGSTOP replica (t=$(el))"; sleep 6; n "$FO2" cont 6379; log "SIGCONT replica" ;;
  lag_netem)
    sleep 5
    sx $FO1 "sudo tc qdisc add dev eth0 root handle 1: prio && sudo tc qdisc add dev eth0 parent 1:3 handle 30: netem delay 1500ms limit 1000000 && sudo tc filter add dev eth0 protocol ip parent 1:0 prio 3 u32 match ip dst $FO2/32 flowid 1:3" \
      && log "netem 1500 ms master -> replica (t=$(el))"
    for i in 1 2 3; do sleep 4; log "lag: master offset $(info $M replication master_repl_offset) replica $(info $R replication slave_repl_offset)"
      $PY "$H/fexp_bulk.py" hide $R --sample 2000 --out "$OUT/hide-$i.json" | tee -a "$OUT/log.txt"; done
    sleep 3; sx $FO1 "sudo tc qdisc del dev eth0 root"; log "netem removed (t=$(el))" ;;
  fullsync)
    sleep 3; F0=$(date +%s.%N)
    n "$FO2" start "$ER" 6379 "replicaof $FO1 6379;appendfsync everysec" >/dev/null
    for i in $(seq 1 6); do sleep 0.3; $PY "$H/fexp_bulk.py" hide $R --sample 300 --out "$OUT/hide-during-sync-$i.json" >> "$OUT/hide-during-sync.txt" 2>&1; done
    waitlink $R 900; F1=$(date +%s.%N)
    log "full sync: replica started t=3.0 s, link up after $(awk -v a=$F0 -v b=$F1 'BEGIN{printf "%.2f", b-a}') s; replica DBSIZE $(rc $R DBSIZE)"
    sx $FO2 "grep -i -E 'sync|load|field' /data/6379/server.log | tail -12" > "$OUT/replica-sync.log" ;;
  promote)
    sleep 12; n "$FO1" kill9 6379; rc $R REPLICAOF NO ONE >/dev/null; log "kill -9 FO1, FO2 promoted (t=$(el))"
    kill $MU 2>/dev/null; wait $MU 2>/dev/null; cp "$OUT/mutate.txt" "$OUT/mutate-1.txt"
    $PY "$H/fexp_bulk.py" mutate $R --seconds 28 > "$OUT/mutate.txt" 2>&1 & MU=$!
    log "FO2 expired_subkeys at promotion: $(info $R stats expired_subkeys)" ;;
  esac
  wait $MU; cat "$OUT/mutate.txt" | tee -a "$OUT/log.txt"
  # every deadline set by load (25 s) and mutate (<= 20 s after its end) passed
  W=$(awk -v a="$T0" -v b="$(date +%s.%N)" 'BEGIN{x=a+62-b; print (x>0?x:0)}'); sleep $W
  if [ "$S" = promote ]; then MM=$R; RR="$R3"; else MM=$M; RR="$R"; [ "$S" = chain ] && RR="$R $R3"; fi
  for r in $RR; do
    waitoff $MM $r || log "WARN offsets not equal ${r}"
    sleep 1
    $PY "$H/fexp_bulk.py" cmp $MM $r --out "$OUT/cmp-${r%:*}.json" | tee -a "$OUT/log.txt"
  done
  log "expired_subkeys: FO1=$(info $M stats expired_subkeys) FO2=$(info $R stats expired_subkeys) FO3=$(info $R3 stats expired_subkeys)"
fi
for h in $FO1 $FO2 $FO3; do sx $h 'grep -E "WARN|ERROR|FATAL|panick|field" /data/6379/server.log 2>/dev/null | tail -20' > "$OUT/warn-$h.log"; done
log "done $S $EM-$ER rep $REP"
