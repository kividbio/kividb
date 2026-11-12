#!/usr/bin/env bash
# eq.sh <rep>: check 3, data equality (1.2.1 master + two 1.2.1 replicas). n2 is attached from the start
# (everything streamed), n3 attaches after the load (full sync) while writes stream; then quiesce and compare
# key by key (kvdiff.py, incl. DEBUG DIGEST-VALUE), then kill -9 + restart n2 and compare again.
set -u
H=$(cd "$(dirname "$0")" && pwd); . "$H/hosts.env"
REP=$1; OUT=${RES:-$HOME/eq-results}/$REP; rm -rf "$OUT"; mkdir -p "$OUT"
PY=$HOME/venv/bin/python
n()  { local h=$1; shift; ssh -o StrictHostKeyChecking=no -o LogLevel=ERROR "$h" "bash node.sh $(printf '%q ' "$@")"; }
rc() { local hp=$1; shift; redis-cli -h "${hp%:*}" -p "${hp#*:}" "$@" 2>&1; }
info() { rc "$1" INFO "$2" | tr -d '\r' | awk -F: -v k="$3" '$1==k{print $2; exit}'; }
waitoff() { for _ in $(seq 1 240); do a=$(info "$1" replication master_repl_offset); b=$(info "$2" replication slave_repl_offset)
  [ -n "$a" ] && [ "$a" = "$b" ] && return 0; sleep 0.5; done; return 1; }
waitlink() { for _ in $(seq 1 300); do [ "$(info "$1" replication master_link_status)" = up ] && return 0; sleep 0.2; done; return 1; }
M=$FO1:6379; R2=$FO2:6379; R3=$FO3:6379
for h in $FO1 $FO2 $FO3; do n "$h" wipeall >/dev/null 2>&1; done
n $FO1 start kividb 6379 "" >/dev/null
n $FO2 start kividb 6379 "replicaof $FO1 6379" >/dev/null; waitlink $R2
$PY "$H/mkdata.py" $M load --n 20000 --dbs 4 --empty-key --nonutf8 --short-field-ttl --ft > "$OUT/load.txt" 2>&1
$PY "$H/mkdata.py" $M stream --seconds 40 --rate 4000 --dbs 4 --nonutf8 --short-field-ttl --prefix w > "$OUT/stream.txt" 2>&1 &
SP=$!
sleep 10; n $FO3 start kividb 6379 "replicaof $FO1 6379" >/dev/null; waitlink $R3 && echo "n3 synced during stream" >> "$OUT/log.txt"
wait $SP
waitoff $M $R2; waitoff $M $R3
sleep 12   # let the short key and field TTLs (<= 7 s) pass on every node
for r in $R2 $R3; do $PY "$H/kvdiff.py" $M $r --out "$OUT/kvdiff-${r%:*}.json" > /dev/null 2>&1; done
n $FO2 kill9 6379; sleep 1; n $FO2 restart 6379 >/dev/null; waitlink $R2; waitoff $M $R2
$PY "$H/kvdiff.py" $M $R2 --out "$OUT/kvdiff-${FO2}-after-restart.json" > /dev/null 2>&1
for hp in $M $R2 $R3; do echo "$hp $(rc $hp INFO keyspace | tr -d '\r' | grep ^db | tr '\n' ' ') sync_full=$(info $hp stats sync_full) sync_partial_ok=$(info $hp stats sync_partial_ok)"; done >> "$OUT/log.txt"
for h in $FO1 $FO2 $FO3; do ssh -o LogLevel=ERROR $h 'grep -E "WARN|ERROR|FATAL|panick" /data/6379/server.log | tail -30' > "$OUT/warn-$h.log"; done
for f in "$OUT"/kvdiff-*.json; do echo "$(basename $f): $(python3 -c "import json,sys;d=json.load(open(sys.argv[1]));print({k:d[k] for k in ('keys_a','keys_b','real_differences','expiring_at_compare','digest_equal','digestvalue_equal','digestvalue_only_differences')}, d['sample'][:5])" $f)"; done >> "$OUT/log.txt"
cat "$OUT/log.txt"
