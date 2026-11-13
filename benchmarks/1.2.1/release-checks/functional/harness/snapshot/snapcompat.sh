#!/usr/bin/env bash
# snapcompat.sh: snapshot / AOF / full-sync compatibility, 1.2.0 (release binary) -> 1.2.1 (35a413c0, release build).
#   env: NEW (default ~/bin/kividb-default), OLD (default /opt/kv120/kividb), OUT (default ~/snap)
# S1  1.2.0 SAVE (no empty key)            -> 1.2.0 restart (control), 1.2.1 start; fingerprints equal
# S2  1.2.0 SAVE with "" in db0 and db5     -> same
# S3  1.2.1 SAVE (dataset, "" in db0/db5, non-UTF-8 keys) -> 1.2.1 restart; equal
# S4  1.2.0 AOF: half the data, BGREWRITEAOF (snapshot + new log), the other half ("" in db5 only in the log) +
#     mutations, 2 s, kill -9 (a) / SHUTDOWN (b) -> 1.2.1 starts on the dir (appendonly yes); equal
# S5  1.2.0 master with the data ("" in db0/db5, FT index), 1.2.1 replica full sync; equal; then mutations streamed;
#     equal; then the replica's own SAVE + restart; equal
# S6  1.2.1 SAVE of text-only data -> 1.2.0 loads it (control: which side moves hash-field deadlines)
# Every refusal is reported with the server's log lines.
set -u
NEW=${NEW:-$HOME/bin/kividb-default}; OLD=${OLD:-/opt/kv120/kividb}; OUT=${OUT:-$HOME/snap}
PY="$HOME/venv/bin/python -I $HOME/snapdata.py"
rm -rf $OUT; mkdir -p $OUT; cd $OUT
exec > >(tee -a $OUT/snapcompat.log) 2>&1
echo "NEW $($NEW --version 2>&1 | head -1) $(sha256sum $NEW | cut -c1-16)"; echo "OLD $($OLD --version 2>&1 | head -1) $(sha256sum $OLD | cut -c1-16)"
PIDS=()
start() { # bin dir port [config lines...] -> 0 if it answers PING (not LOADING) within 120 s
  local bin=$1 d=$2 port=$3; shift 3
  printf '%s
' "$@" > $d/kividb.conf
  (cd $d && exec $bin --port $port --protected-mode no --dir $d --configfile $d/kividb.conf >> $d/server.log 2>&1 < /dev/null) &
  echo $! > $d/pid
  for i in $(seq 1 1200); do
    local pong=$(redis-cli -p $port PING 2>/dev/null); [ "$pong" = PONG ] && return 0
    kill -0 $(cat $d/pid) 2>/dev/null || { echo "  !! server exited: $(tail -5 $d/server.log | tr '\n' '|')"; return 1; }
    sleep 0.1
  done; echo "  !! no PONG in 120 s"; return 1
}
stop9() { kill -9 $(cat $1/pid) 2>/dev/null; sleep 0.5; }
loadlines() { grep -a -iE "load|snapshot|aof|replay|refus|error|warn|fatal|panic" $1/server.log | grep -v -i "update check" | tail -${2:-8} | sed 's/^/    log: /'; }
pass=0; fail=0
check() { # label a.json b.json   (PASS = no real difference; known classes are counted in the DIFF line)
  if $PY diff $2 $3 > $OUT/diff-$1.txt; then pass=$((pass+1)); echo "PASS $1: $(head -1 $OUT/diff-$1.txt | sed 's/^DIFF [^:]*: //')"
  else fail=$((fail+1)); echo "FAIL $1:"; head -20 $OUT/diff-$1.txt; fi
}

for S in S1 S2; do
  EK=""; [ $S = S2 ] && EK="--empty-key"
  echo "=== $S: 1.2.0 writes dump.kdb ${EK:-without the empty key}"
  d=$OUT/$S-old; mkdir -p $d
  start $OLD $d 7601 "appendonly no" || { fail=$((fail+1)); continue; }
  $PY load 7601 $EK | tee $OUT/$S-load.json | cut -c1-600
  $PY fp 7601 $OUT/$S-old.json | cut -c1-300
  echo "  SAVE: $(redis-cli -p 7601 SAVE)"; stop9 $d; ls -la $d/dump.kdb | awk '{print "  dump.kdb", $5, "bytes"}'
  for who in old new; do
    e=$OUT/$S-$who-load; mkdir -p $e; cp $d/dump.kdb $e/; B=$OLD; [ $who = new ] && B=$NEW
    echo "  -> $who ($B) starts on that dump.kdb"
    if start $B $e 7602 "appendonly no"; then
      echo "  keys: $(for db in 0 5 9; do printf 'db%s=%s ' $db $(redis-cli -p 7602 -n $db DBSIZE); done)"
      $PY fp 7602 $OUT/$S-$who-load.json > /dev/null; check $S-$who-loads-1.2.0-dump $OUT/$S-old.json $OUT/$S-$who-load.json
      if [ $who = new ]; then
        echo "  1.2.1 SAVE + restart on its own file: $(redis-cli -p 7602 SAVE)"; stop9 $e
        start $B $e 7602 "appendonly no" && { $PY fp 7602 $OUT/$S-new-reload.json > /dev/null; check $S-new-resave-reload $OUT/$S-old.json $OUT/$S-new-reload.json; }
      fi
    else
      [ $who = new ] && fail=$((fail+1)); echo "  REFUSED ($who):"; loadlines $e 12
    fi
    loadlines $e 4; stop9 $e
  done
done

echo "=== S3: 1.2.1 writes dump.kdb (\"\" in db0/db5, non-UTF-8 keys) and loads it"
d=$OUT/S3; mkdir -p $d
start $NEW $d 7603 "appendonly no" && {
  $PY load 7603 --empty-key --nonutf8 | cut -c1-600; $PY fp 7603 $OUT/S3-a.json | cut -c1-300
  echo "  SAVE: $(redis-cli -p 7603 SAVE)"; stop9 $d
  start $NEW $d 7603 "appendonly no" && { $PY fp 7603 $OUT/S3-b.json > /dev/null; check S3-new-own-dump $OUT/S3-a.json $OUT/S3-b.json; } || fail=$((fail+1))
  loadlines $d 4; stop9 $d
  e=$OUT/S3-old; mkdir -p $e; cp $d/dump.kdb $e/
  echo "  (documented) 1.2.0 on this 1.2.1 file:"; start $OLD $e 7604 "appendonly no" && echo "    1.2.0 started (unexpected)"; loadlines $e 3; stop9 $e
}

for v in a b; do
  echo "=== S4$v: 1.2.0 AOF: snapshot + log, then $( [ $v = a ] && echo 'kill -9' || echo 'SHUTDOWN') -> 1.2.1 starts on the dir"
  d=$OUT/S4$v-old; mkdir -p $d
  start $OLD $d 7605 "appendonly yes" "appendfsync everysec" || { fail=$((fail+1)); continue; }
  $PY load 7605 --empty-key --part a | cut -c1-400
  echo "  BGREWRITEAOF: $(redis-cli -p 7605 BGREWRITEAOF)"
  for i in $(seq 1 600); do redis-cli -p 7605 INFO persistence | grep -q 'aof_rewrite_in_progress:0' && break; sleep 0.1; done
  redis-cli -p 7605 INFO persistence | grep -E 'aof_last_bgrewrite_status|aof_rewrites' | tr -d '\r' | tr '\n' ' '; echo
  $PY load 7605 --empty-key --part b | cut -c1-400
  $PY mutate 7605 | cut -c1-300
  $PY fp 7605 $OUT/S4$v-old.json | cut -c1-300
  sleep 2
  if [ $v = a ]; then stop9 $d; else echo "  SHUTDOWN: $(redis-cli -p 7605 SHUTDOWN 2>&1)"; sleep 1; fi
  ls -la $d | awk 'NR>1 && $9 !~ /^\.|server.log|pid/ {print "  file", $9, $5}'
  echo "  appendonly.aof starts with: $(head -c 24 $d/appendonly.aof | od -An -c | tr -s ' ' | head -2 | tr '\n' ' ')"
  e=$OUT/S4$v-new; mkdir -p $e; cp -a $d/. $e/; rm -f $e/server.log $e/pid $e/kividb.conf
  if start $NEW $e 7606 "appendonly yes" "appendfsync everysec"; then
    $PY fp 7606 $OUT/S4$v-new.json > /dev/null; check S4$v-new-loads-1.2.0-aof $OUT/S4$v-old.json $OUT/S4$v-new.json
  else fail=$((fail+1)); echo "  REFUSED:"; loadlines $e 12; fi
  loadlines $e 6; stop9 $e
done

echo "=== S5: full sync 1.2.0 master -> 1.2.1 replica"
m=$OUT/S5-master; r=$OUT/S5-replica; mkdir -p $m $r
start $OLD $m 7607 "appendonly yes" && start $NEW $r 7608 "appendonly yes" && {
  $PY load 7607 --empty-key | cut -c1-400
  redis-cli -p 7608 REPLICAOF 127.0.0.1 7607
  for i in $(seq 1 600); do redis-cli -p 7608 INFO replication | grep -q 'master_link_status:up' && break; sleep 0.2; done
  syncwait() { for i in $(seq 1 300); do a=$(redis-cli -p 7607 INFO replication | tr -d '\r' | awk -F: '/^master_repl_offset/{print $2}'); b=$(redis-cli -p 7608 INFO replication | tr -d '\r' | awk -F: '/^slave_repl_offset/{print $2}'); [ "$a" = "$b" ] && return 0; sleep 0.2; done; return 1; }
  syncwait; sleep 1
  echo "  link: $(redis-cli -p 7608 INFO replication | tr -d '\r' | grep -E 'master_link_status|slave_repl_offset' | tr '\n' ' ') master sync_full=$(redis-cli -p 7607 INFO stats | tr -d '\r' | awk -F: '/^sync_full/{print $2}')"
  $PY fp 7607 $OUT/S5-m1.json | cut -c1-300; $PY fp 7608 $OUT/S5-r1.json > /dev/null; check S5-full-sync $OUT/S5-m1.json $OUT/S5-r1.json
  $PY mutate 7607 > /dev/null; syncwait; sleep 1
  $PY fp 7607 $OUT/S5-m2.json > /dev/null; $PY fp 7608 $OUT/S5-r2.json > /dev/null; check S5-stream-after-sync $OUT/S5-m2.json $OUT/S5-r2.json
  redis-cli -p 7608 REPLICAOF NO ONE > /dev/null; echo "  replica SAVE: $(redis-cli -p 7608 SAVE)"; stop9 $r
  start $NEW $r 7608 "appendonly yes" && { $PY fp 7608 $OUT/S5-r3.json > /dev/null; check S5-replica-restart $OUT/S5-m2.json $OUT/S5-r3.json; } || fail=$((fail+1))
  loadlines $r 6; stop9 $r; stop9 $m
} || { fail=$((fail+1)); echo "S5 could not start"; }
echo "=== S6: 1.2.1 writes a text-only dump.kdb (no empty key) -> 1.2.0 loads it (isolates 1.2.0's reader)"
d=$OUT/S6; mkdir -p $d
start $NEW $d 7609 "appendonly no" && {
  $PY load 7609 | cut -c1-300; $PY fp 7609 $OUT/S6-a.json > /dev/null; echo "  SAVE: $(redis-cli -p 7609 SAVE)"; stop9 $d
  e=$OUT/S6-old; mkdir -p $e; cp $d/dump.kdb $e/
  if start $OLD $e 7610 "appendonly no"; then $PY fp 7610 $OUT/S6-b.json > /dev/null; check S6-old-loads-1.2.1-text-dump $OUT/S6-a.json $OUT/S6-b.json
  else echo "  REFUSED (1.2.0 on a text-only 1.2.1 file):"; loadlines $e 6; fi
  stop9 $e
}
echo "SNAPCOMPAT pass=$pass fail=$fail"
