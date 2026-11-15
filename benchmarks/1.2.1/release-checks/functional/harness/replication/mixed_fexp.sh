#!/usr/bin/env bash
# mixed_fexp.sh: hash-field TTLs across 1.2.0 <-> 1.2.1 (client VM). CHANGELOG [Unreleased] #415 says: a 1.2.1
# replica of a 1.2.0 master (which sends no HDEL) still removes fields by its own clock, and the gap stays
# until the master is upgraded too.
#   A  1.2.0 master FO1 -> 1.2.1 replica FO2: log line, race (fexp_ext), bulk expiry + writes, cmp
#   B  FO1 upgraded in place to 1.2.1 (SIGTERM, same data): FO2 reconnects; race and bulk again (new keys)
#   C  1.2.1 master FO1 -> 1.2.0 replica FO3 (fresh full sync): race, bulk, cmp
set -u
H=$(cd "$(dirname "$0")" && pwd); . "$H/hosts.env"
OUT=${RES:-$HOME/mixed-fexp}; rm -rf "$OUT"; mkdir -p "$OUT"
PY=$HOME/venv/bin/python
n()  { local h=$1; shift; ssh -o StrictHostKeyChecking=no -o LogLevel=ERROR "$h" "bash node.sh $(printf '%q ' "$@")"; }
sx() { local h=$1; shift; ssh -o StrictHostKeyChecking=no -o LogLevel=ERROR "$h" "$@"; }
rc() { local hp=$1; shift; redis-cli -h "${hp%:*}" -p "${hp#*:}" "$@" 2>&1; }
info() { rc "$1" INFO "$2" | tr -d '\r' | awk -F: -v k="$3" '$1==k{print $2; exit}'; }
log() { echo "$(date +%T) $*" | tee -a "$OUT/log.txt"; }
waitlink() { for _ in $(seq 1 600); do [ "$(info "$1" replication master_link_status)" = up ] && return 0; sleep 0.1; done; return 1; }
waitoff() { for _ in $(seq 1 240); do a=$(info "$1" replication master_repl_offset); b=$(info "$2" replication slave_repl_offset)
  [ -n "$a" ] && [ "$a" = "$b" ] && return 0; sleep 0.5; done; return 1; }
M=$FO1:6379; R=$FO2:6379; R3=$FO3:6379
RACE=HPEXPIRE,HGETEX_PX,HPERSIST,HINCRBY,HSETNX,HSETEX_KEEPTTL,P_HPEXPIRE,P_HPEXPIREAT,P_HSETEX_PX
bulk() { # bulk <tag> <master> <replica> <prefix>
  $PY "$H/fexp_bulk.py" load $2 --hashes 10000 --min-ms 2000 --max-ms 15000 --prefix $4 | tee -a "$OUT/log.txt"
  $PY "$H/fexp_bulk.py" mutate $2 --seconds 20 --hashes 10000 --prefix $4 | tee -a "$OUT/log.txt"
  sleep 22; waitoff $2 $3; sleep 1
  $PY "$H/fexp_bulk.py" cmp $2 $3 --prefix $4 --out "$OUT/cmp-$1.json" | tee -a "$OUT/log.txt"
}
for h in $FO1 $FO2 $FO3; do n "$h" wipeall >/dev/null 2>&1; done
# ---- A
n "$FO1" start kv120 6379 "appendfsync everysec" >/dev/null
n "$FO2" start kividb 6379 "replicaof $FO1 6379;appendfsync everysec" >/dev/null; waitlink $R
log "A: master $(info $M server kividb_version) flag=[$(info $M server kividb_expired_fields_propagated)]; replica $(info $R server kividb_version)"
sx $FO2 "grep -i 'hash fields' /data/6379/server.log" | tee -a "$OUT/log.txt"
$PY "$H/fexp_ext.py" $M $R --n 300 --modes $RACE --out "$OUT/race-A.json" | tee -a "$OUT/log.txt"
bulk A $M $R fa
log "A: expired_subkeys master=$(info $M stats expired_subkeys) replica=$(info $R stats expired_subkeys)"
# ---- B: upgrade the master in place
n "$FO1" term 6379; n "$FO1" setbin 6379 kividb; n "$FO1" restart 6379 >/dev/null
sleep 1; waitlink $R
log "B: master now $(info $M server kividb_version) flag=[$(info $M server kividb_expired_fields_propagated)]; replica link $(info $R replication master_link_status) sync_full(master)=$(info $M stats sync_full) psync_ok=$(info $M stats sync_partial_ok)"
sx $FO2 "grep -i -E 'hash fields|full|partial|resync' /data/6379/server.log | tail -6" | tee -a "$OUT/log.txt"
$PY "$H/fexp_ext.py" $M $R --n 300 --modes $RACE --out "$OUT/race-B.json" | tee -a "$OUT/log.txt"
bulk B $M $R fbb
# ---- C: 1.2.0 replica of the 1.2.1 master
n "$FO3" start kv120 6379 "replicaof $FO1 6379;appendfsync everysec" >/dev/null; waitlink $R3
log "C: replica FO3 $(info $R3 server kividb_version) link $(info $R3 replication master_link_status)"
waitoff $M $R3; $PY "$H/kvdiff.py" $M $R3 --no-digest-value --out "$OUT/kvdiff-C-sync.json" | tail -2 | tee -a "$OUT/log.txt"
$PY "$H/fexp_ext.py" $M $R3 --n 300 --modes $RACE --out "$OUT/race-C.json" | tee -a "$OUT/log.txt"
bulk C $M $R3 fc
log "C: expired_subkeys master=$(info $M stats expired_subkeys) 1.2.0 replica=$(info $R3 stats expired_subkeys)"
sx $FO3 "grep -E 'WARN|ERROR' /data/6379/server.log | sort | uniq -c | sort -rn | head -10" | tee -a "$OUT/log.txt"
log "done"
