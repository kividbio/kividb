#!/usr/bin/env bash
# mixed.sh <test>: mixed-version replication (runs on the client VM; kividb = 1.2.1 @878746e4, kv120 = released 1.2.0)
#   a        1.2.0 master -> 1.2.1 replica: full sync (empty key in every db, FT index), streamed writes,
#            FT.DROPINDEX from another database, negative HEXPIREAT
#   b        1.2.1 master -> 1.2.0 replica: text-only sync + stream; streamed non-UTF-8 key; full sync with a
#            non-UTF-8 key; full sync with the empty key
#   back     going back: a 1.2.1 snapshot / AOF with a non-UTF-8 key, or the empty key, started on 1.2.0
set -u
H=$(cd "$(dirname "$0")" && pwd)
. "$H/hosts.env"
T=$1
OUT=${RES:-$HOME/mixed-results}/$T; rm -rf "$OUT"; mkdir -p "$OUT"
PY=$HOME/venv/bin/python
n()  { local h=$1; shift; ssh -o StrictHostKeyChecking=no -o LogLevel=ERROR "$h" "bash node.sh $(printf '%q ' "$@")"; }
sx() { local h=$1; shift; ssh -o StrictHostKeyChecking=no -o LogLevel=ERROR "$h" "$@"; }
rc() { local hp=$1; shift; redis-cli -h "${hp%:*}" -p "${hp#*:}" "$@" 2>&1; }
kv() { $PY "$H/kv.py" "$@"; }
info() { rc "$1" INFO "$2" | tr -d '\r' | awk -F: -v k="$3" '$1==k{print $2; exit}'; }
log() { echo "$*" | tee -a "$OUT/log.txt"; }
check() { # check <name> <expected> <actual>
  if [ "$2" = "$3" ]; then log "MATCH   $1: $3"; else log "MISMATCH $1: expected [$2] got [$3]"; fi; }
waitlink() { for _ in $(seq 1 ${2:-150}); do [ "$(info "$1" replication master_link_status)" = up ] && return 0; sleep 0.2; done; return 1; }
waitoff() { for _ in $(seq 1 120); do a=$(info "$1" replication master_repl_offset); b=$(info "$2" replication slave_repl_offset)
  [ -n "$a" ] && [ "$a" = "$b" ] && return 0; sleep 0.5; done; return 1; }
alive() { rc "$1" PING | grep -q PONG && echo alive || echo DEAD; }
M=$FO1:6379; R=$FO2:6379
for h in $FO1 $FO2 $FO3; do n "$h" wipeall >/dev/null 2>&1; done

case $T in
a)
  n "$FO1" start kv120 6379 "appendfsync always" >/dev/null
  log "master: $(rc $M INFO server | tr -d '\r' | grep -m1 kividb_version)"
  $PY "$H/mkdata.py" $M load --n 3000 --dbs 3 --empty-key --ft --text-only | tee -a "$OUT/log.txt"
  n "$FO2" start kividb 6379 "replicaof $FO1 6379" >/dev/null
  waitlink $R && log "1.2.1 replica: link up after full sync" || log "MISMATCH 1.2.1 replica link not up"
  sx $FO2 "grep -i -E 'sync|load|error|warn' /data/6379/server.log | tail -20" > "$OUT/replica-fullsync.log"
  $PY "$H/mkdata.py" $M stream --seconds 20 --rate 3000 --dbs 3 --text-only --prefix s | tee -a "$OUT/log.txt"
  waitoff $M $R
  $PY "$H/kvdiff.py" $M $R --no-digest-value --out "$OUT/kvdiff-1.json" | tee -a "$OUT/log.txt"
  check "empty key db0 on replica" "b'empty-key-db0'" "$(kv $R 0 GET '')"
  check "empty key db1 on replica" "b'empty-key-db1'" "$(kv $R 1 GET '')"
  check "empty key db2 on replica" "b'empty-key-db2'" "$(kv $R 2 GET '')"
  check "FT index on replica (FT._LIST db0)" "[b'idx0']" "$(kv $R 0 FT._LIST)"
  log "FT.SEARCH replica: $(kv $R 0 FT.SEARCH idx0 hello LIMIT 0 0)  master: $(kv $M 0 FT.SEARCH idx0 hello LIMIT 0 0)"
  # FT.DROPINDEX sent from database 1 for db 0's index (1.2.0 drops it; 1.2.1 refuses it when it applies it)
  log "master (1.2.0) FT.DROPINDEX idx0 from db 1: $(kv $M 1 FT.DROPINDEX idx0)"
  sleep 1
  check "master FT._LIST db0 after (1.2.0 dropped it)" "[]" "$(kv $M 0 FT._LIST)"
  check "replica drops idx0 too (documented since dd11dddc: 1.2.0 writes it under the index db)" "[]" "$(kv $R 0 FT._LIST)"
  check "replica link still up" "up" "$(info $R replication master_link_status)"
  sx $FO2 "grep -i -E 'dropindex|index|refus|warn|error' /data/6379/server.log | tail -10" > "$OUT/replica-dropindex.log"
  # negative HEXPIREAT
  kv $M 0 HSET hneg f1 v1 f2 v2 >/dev/null
  log "master (1.2.0) HEXPIREAT hneg -5 FIELDS 1 f1: $(kv $M 0 HEXPIREAT hneg -5 FIELDS 1 f1)"
  kv $M 0 HPEXPIREAT hneg -1 FIELDS 1 nosuch >/dev/null
  sleep 1
  check "master hneg fields (1.2.0 deleted f1)" "[b'f2']" "$(kv $M 0 HKEYS hneg)"
  check "replica hneg fields (applied as 1.2.0 did)" "[b'f2']" "$(kv $R 0 HKEYS hneg)"
  # and as a full sync: restart the 1.2.1 replica after a master restart (new replid -> full sync)
  n "$FO1" kill9 6379; sleep 0.5; n "$FO1" restart 6379 >/dev/null
  sleep 2; waitlink $R
  log "replica full syncs: $(info $R stats sync_full) master sync_full=$(info $M stats sync_full)"
  waitoff $M $R
  $PY "$H/kvdiff.py" $M $R --no-digest-value --out "$OUT/kvdiff-2.json" | tee -a "$OUT/log.txt"
  check "replica still alive" alive "$(alive $R)"
  check "1.2.0 master: idx0 after its own AOF replay" "[]" "$(kv $M 0 FT._LIST)"
  check "replica after second full sync: FT._LIST" "[]" "$(kv $R 0 FT._LIST)"
  ;;
a2)  # the other FT commands of the upgrade note, sent from database 1 for database 0's index, and AOF replay
  n "$FO1" start kv120 6379 "appendfsync always" >/dev/null
  n "$FO2" start kividb 6379 "replicaof $FO1 6379" >/dev/null
  waitlink $R
  for i in $(seq 0 49); do kv $M 0 HSET doc:$i title "hello $i" >/dev/null; done
  kv $M 0 FT.CREATE idx0 ON HASH PREFIX 1 doc: SCHEMA title TEXT >/dev/null
  kv $M 0 FT.CREATE idx9 ON HASH PREFIX 1 nodoc: SCHEMA title TEXT >/dev/null
  sleep 1
  ftinfo() { kv $1 0 FT.INFO idx0 | grep -o -E "b'attributes', \[[^]]*\]\]|attributes[^]]*" | head -c 300; }
  log "master FT.ALTER idx0 from db1: $(kv $M 1 FT.ALTER idx0 SCHEMA ADD extra TEXT)"
  sleep 0.5
  log "  master idx0 attrs: $(kv $M 0 FT.INFO idx0 | grep -o -c extra) mentions of 'extra'; replica: $(kv $R 0 FT.INFO idx0 | grep -o -c extra)"
  log "master FT.ALIASADD al0 idx0 from db1: $(kv $M 1 FT.ALIASADD al0 idx0)"
  sleep 0.5
  log "  FT.SEARCH al0 from db0: master $(kv $M 0 FT.SEARCH al0 hello LIMIT 0 0) replica $(kv $R 0 FT.SEARCH al0 hello LIMIT 0 0)"
  log "master FT.ALIASUPDATE al0 idx9 from db1: $(kv $M 1 FT.ALIASUPDATE al0 idx9)"
  sleep 0.5
  log "  FT.SEARCH al0 from db0: master $(kv $M 0 FT.SEARCH al0 hello LIMIT 0 0) replica $(kv $R 0 FT.SEARCH al0 hello LIMIT 0 0)"
  log "master FT.DROPINDEX idx0 from db1: $(kv $M 1 FT.DROPINDEX idx0)"
  sleep 0.5
  log "  FT._LIST db0: master $(kv $M 0 FT._LIST) replica $(kv $R 0 FT._LIST)"
  sx $FO1 "grep -a -n -B6 -i -E '^FT\\.(ALTER|ALIAS|DROP)' /data/6379/appendonly.aof | tr -d '\\r' | tr '\\n' ' '" | head -c 2000 > "$OUT/a2-master-aof.txt"
  log "  1.2.0 master's AOF around them: $(cat "$OUT/a2-master-aof.txt")"
  # AOF replay on 1.2.1: copy the 1.2.0 master's AOF to FO3 and start 1.2.1 on it
  sx $FO1 "cat /data/6379/appendonly.aof" > /tmp/a2.aof
  n "$FO3" start kividb 6379 "appendfsync always" >/dev/null; n "$FO3" term 6379
  scp -q /tmp/a2.aof $FO3:/data/6379/appendonly.aof; sx $FO3 "rm -f /data/6379/dump.kdb; rm -rf /data/6379/appendonlydir"
  n "$FO3" restart 6379 >/dev/null; sleep 2
  log "  1.2.1 started on the 1.2.0 AOF: FT._LIST $(kv $FO3:6379 0 FT._LIST); log: $(sx $FO3 "grep -i -E 'refus|index|warn' /data/6379/server.log | tail -4" | cut -c1-250 | tr '\n' ' ')"
  ;;
b)
  n "$FO1" start kividb 6379 "appendfsync always" >/dev/null
  $PY "$H/mkdata.py" $M load --n 3000 --dbs 3 --text-only | tee -a "$OUT/log.txt"
  n "$FO2" start kv120 6379 "replicaof $FO1 6379" >/dev/null
  waitlink $R && log "MATCH   1.2.0 replica of 1.2.1 master: link up" || log "MISMATCH 1.2.0 replica link not up"
  $PY "$H/mkdata.py" $M stream --seconds 20 --rate 3000 --dbs 3 --text-only --prefix s | tee -a "$OUT/log.txt"
  waitoff $M $R
  $PY "$H/kvdiff.py" $M $R --no-digest-value --out "$OUT/kvdiff-text.json" | tee -a "$OUT/log.txt"
  # b1: a replicated write of a non-UTF-8 key (and a hash, and a DEL of one) while streaming
  kv $M 0 SET 'before' 1 >/dev/null
  log "master SET \\xff\\xfe: $(kv $M 0 SET '\xff\xfe' v) HSET h\\xff: $(kv $M 0 HSET 'h\xff' f v) SET k \\xff-value: $(kv $M 0 SET 'kbinval' '\xff')"
  kv $M 0 SET 'after' 2 >/dev/null
  sleep 1
  check "1.2.0 replica dropped SET of non-UTF-8 key" "None" "$(kv $R 0 GET '\xff\xfe')"
  check "1.2.0 replica took the next write" "b'2'" "$(kv $R 0 GET after)"
  check "1.2.0 replica took a non-UTF-8 value" "b'\\xff'" "$(kv $R 0 GET kbinval)"
  check "1.2.0 replica link up" up "$(info $R replication master_link_status)"
  check "1.2.0 replica alive" alive "$(alive $R)"
  sx $FO2 "grep -n -i -E 'warn|utf|drop|skip' /data/6379/server.log | tail -10" > "$OUT/b1-replica-warnings.log"
  log "b1 replica warnings: $(wc -l < "$OUT/b1-replica-warnings.log") lines: $(head -3 "$OUT/b1-replica-warnings.log" | cut -c1-220)"
  kv $M 0 DEL '\xff\xfe' 'h\xff' >/dev/null; sleep 0.5
  waitoff $M $R
  $PY "$H/kvdiff.py" $M $R --no-digest-value --out "$OUT/kvdiff-b1.json" | tee -a "$OUT/log.txt"
  # b2: full sync with a non-UTF-8 key in the master's data set. Restart the master (new replid -> full sync).
  RD=$(rc $R DBSIZE); kv $R 0 GET after > "$OUT/b2-replica-before.txt"
  $PY "$H/kvdiff.py" $M $R --no-digest-value > "$OUT/b2-pre.json"
  kv $M 0 SET '\xffbad' v >/dev/null; sleep 0.5
  F0=$(info $M stats sync_full)
  n "$FO1" kill9 6379; sleep 0.5; n "$FO1" restart 6379 >/dev/null
  kv $M 0 SET 'post-restart' 1 >/dev/null
  sleep 20
  F1=$(info $M stats sync_full)
  log "b2 master sync_full: before restart $F0 (counter restarts at 0), 20 s after restart: $F1"
  check "b2 1.2.0 replica alive" alive "$(alive $R)"
  check "b2 1.2.0 replica link not up (refuses the sync)" "down" "$(info $R replication master_link_status)"
  check "b2 1.2.0 replica keeps its own data (DBSIZE)" "$RD" "$(rc $R DBSIZE)"
  check "b2 1.2.0 replica keeps its own data (no post-restart key)" "None" "$(kv $R 0 GET post-restart)"
  sx $FO2 "grep -c -i -E 'full sync|fullresync|sync' /data/6379/server.log; grep -i -E 'utf|refus|fail|error' /data/6379/server.log | tail -8" > "$OUT/b2-replica.log"
  log "b2 replica log: $(head -c 1500 "$OUT/b2-replica.log")"
  sx $FO2 "ps -o rss=,vsz= -p \$(cat /data/6379/pid)" > "$OUT/b2-replica-rss.txt"; log "b2 replica rss/vsz KB: $(cat "$OUT/b2-replica-rss.txt")"
  # recovery: delete the key on the master; the next full sync must be taken
  kv $M 0 DEL '\xffbad' >/dev/null
  waitlink $R 300 && log "MATCH   b2 replica syncs once the key is gone" || log "MISMATCH b2 replica still not syncing after DEL"
  waitoff $M $R; $PY "$H/kvdiff.py" $M $R --no-digest-value --out "$OUT/kvdiff-b2-after.json" | tee -a "$OUT/log.txt"
  # b3: full sync with the empty key in the master's data set (streamed first: 1.2.0 takes the key "" itself)
  kv $M 2 SET '' empty >/dev/null; sleep 0.5
  check "b3 streamed empty key reached the 1.2.0 replica" "b'empty'" "$(kv $R 2 GET '')"
  kv $M 0 SET '' empty0 >/dev/null; sleep 0.5
  RD=$(rc $R DBSIZE)
  n "$FO1" kill9 6379; sleep 0.5; n "$FO1" restart 6379 >/dev/null
  kv $M 0 SET 'post-restart-2' 1 >/dev/null
  sleep 20
  log "b3 master sync_full 20 s after restart: $(info $M stats sync_full)"
  check "b3 1.2.0 replica alive" alive "$(alive $R)"
  check "b3 1.2.0 replica link not up (refuses the sync)" "down" "$(info $R replication master_link_status)"
  check "b3 1.2.0 replica keeps its own data (DBSIZE)" "$RD" "$(rc $R DBSIZE)"
  check "b3 1.2.0 replica keeps its own data (no post-restart key)" "None" "$(kv $R 0 GET post-restart-2)"
  sx $FO2 "grep -i -E 'utf|refus|fail|error|GiB|length' /data/6379/server.log | tail -8" > "$OUT/b3-replica.log"
  log "b3 replica log: $(head -c 1500 "$OUT/b3-replica.log")"
  sx $FO2 "ps -o rss=,vsz= -p \$(cat /data/6379/pid)" > "$OUT/b3-replica-rss.txt"; log "b3 replica rss/vsz KB: $(cat "$OUT/b3-replica-rss.txt")"
  kv $M 0 DEL '' >/dev/null; kv $M 2 DEL '' >/dev/null
  waitlink $R 300 && log "MATCH   b3 replica syncs once the empty keys are gone" || log "MISMATCH b3 replica still not syncing"
  waitoff $M $R; $PY "$H/kvdiff.py" $M $R --no-digest-value --out "$OUT/kvdiff-b3-after.json" | tee -a "$OUT/log.txt"
  sx $FO2 "cat /data/6379/server.log" | tail -300 > "$OUT/replica-server.log"
  ;;
back)
  for mode in snap aof; do for what in nonutf8 empty; do
    if [ $mode = snap ]; then X="appendonly no"; else X="appendonly yes"; fi
    n "$FO3" wipe 6379 >/dev/null 2>&1
    n "$FO3" start kividb 6379 "$X" >/dev/null
    H3=$FO3:6379
    $PY "$H/mkdata.py" $H3 load --n 500 --dbs 2 --text-only > /dev/null
    if [ $what = nonutf8 ]; then kv $H3 0 SET '\xff' v >/dev/null; else kv $H3 1 SET '' v >/dev/null; fi
    if [ $mode = snap ]; then kv $H3 0 SAVE >/dev/null; else kv $H3 0 BGREWRITEAOF >/dev/null; sleep 3; fi
    n "$FO3" term 6379
    sx $FO3 "ls -la /data/6379/ /data/6379/appendonlydir 2>/dev/null" > "$OUT/back-$mode-$what-files.txt"
    n "$FO3" setbin 6379 kv120; sx $FO3 ": > /data/6379/server.log"; n "$FO3" restart 6379 >/dev/null; sleep 5
    st=$(rc $H3 PING | tr -d '\r'); [ -z "$st" ] && st=none
    sx $FO3 "tail -5 /data/6379/server.log" > "$OUT/back-$mode-$what.log"
    sx $FO3 "kill -0 \$(cat /data/6379/pid) 2>/dev/null && echo running || echo exited" > "$OUT/back-$mode-$what-proc.txt"
    log "back $mode $what: 1.2.0 PING=[$st] process=$(cat "$OUT/back-$mode-$what-proc.txt") log: $(tail -2 "$OUT/back-$mode-$what.log" | cut -c1-300 | tr '\n' ' ')"
    sx $FO3 "ls -la /data/6379/" > "$OUT/back-$mode-$what-files-after.txt"
    n "$FO3" kill9 6379 2>/dev/null
  done; done
  ;;
esac
log "done $T"
