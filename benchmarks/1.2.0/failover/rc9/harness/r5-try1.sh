#!/usr/bin/env bash
# r5-try1.sh (the first item-5 run, which hung at its first clean stop; kept as run): item 5, a replica stopped (SIGTERM) in the middle of a full sync keeps its last good dump.
# Master t1 + replica t2 (server.sh: AOF on, everysec; repl-backlog-size at its default, 1 MB). KEYS (3.4 M) string
# keys prefilled, then the rc7 soak mix at 100k ops/s for the whole run. Each round (one per DELAYS entry):
#   1. the replica, in step, is stopped cleanly (SIGTERM): the sha256 of its dump.kdb and dump.kdb.repl are the good ones;
#   2. 5 s of load (the 1 MB backlog no longer holds its offset), then it is started again: it must full-sync;
#   3. DELAY s after its master_sync_in_progress turns 1, SIGTERM; wait for the exit;
#   4. dump.kdb and dump.kdb.repl must be byte-identical to the good ones, and its log must say the final save was
#      skipped; then it is started again and must sync (link up, sync done).
# At the end the load stops, the offsets must meet, and DBSIZE must be equal. Output in ~/r5 (or $OUT).
set -u
. $HOME/hosts.env
OUT=${OUT:-$HOME/r5}; rm -rf $OUT; mkdir -p $OUT
KEYS=${KEYS:-3400000} DELAYS=${DELAYS:-"0.2 3 8"}
s() { ssh -o LogLevel=ERROR "$@"; }
inf() { timeout 2 redis-cli -h $1 INFO $2 2>/dev/null | tr -d '\r' | awk -F: -v k=$3 '$1==k{print $2}'; }
el() { awk -v a=$1 -v b=$(date +%s.%N) 'BEGIN{printf "%.2f", b-a}'; }
STOP='p=$(cat /mnt/kv/pid); kill -TERM $p; for i in $(seq 1 1200); do kill -0 $p 2>/dev/null || break; sleep 0.05; done; kill -0 $p 2>/dev/null && echo STILL-RUNNING'
START='(cd /mnt/kv && exec nohup $(head -1 /mnt/kv/cmd) >> /mnt/kv/server.log 2>&1 < /dev/null) & echo $! > /mnt/kv/pid'
SUMS='cd /mnt/kv; sha256sum dump.kdb dump.kdb.repl 2>&1; ls -la --time-style=+%T.%N | grep -v "^total"'
in_step() { for _ in $(seq 1 1200); do [ "$(inf $R replication master_link_status)" = up ] && [ "$(inf $R replication master_sync_in_progress)" = 0 ] && return 0; sleep 0.5; done; return 1; }
s $M "ENGINE=kividb bash server.sh start-master" > $OUT/start.txt
s $R "ENGINE=kividb bash server.sh start-replica $M" >> $OUT/start.txt
sleep 2
$HOME/soakgen -addr $M:6379 -conns 64 -prefill -str-keys $KEYS
in_step
echo "prefilled: master keys $(redis-cli -h $M DBSIZE), replica keys $(redis-cli -h $R DBSIZE)" | tee $OUT/result.txt
$HOME/soakgen -addr $M:6379 -conns 128 -rate 100000 -duration 60m -str-keys $KEYS > $OUT/soak.csv 2> $OUT/soakgen.err &
SG=$!
sleep 20
echo "round,delay_s,sync_full_added,sync_in_progress_at_kill,loading_at_kill,exit_s,dump_kdb_same,dump_kdb_repl_same,final_save_skipped_logged,synced_again_s" > $OUT/rounds.csv
i=0
for d in $DELAYS; do
  i=$((i+1))
  in_step; sleep 3
  s $R "echo '=== round $i: clean stop $(date -u +%FT%T.%3NZ)' >> /mnt/kv/server.log; $STOP"
  s $R "$SUMS" > $OUT/r$i-good.txt
  sleep 5
  f0=$(inf $M stats sync_full)
  s $R "echo '=== round $i: start, full sync expected $(date -u +%FT%T.%3NZ)' >> /mnt/kv/server.log; $START"
  seen=""
  for _ in $(seq 1 1200); do [ "$(inf $R replication master_sync_in_progress)" = 1 ] && { seen=1; break; }; sleep 0.05; done
  sleep $d
  sip=$(inf $R replication master_sync_in_progress); ld=$(inf $R persistence loading)
  t0=$(date +%s.%N)
  s $R "echo '=== round $i: SIGTERM mid sync $(date -u +%FT%T.%3NZ)' >> /mnt/kv/server.log; $STOP" > $OUT/r$i-stop.txt
  ex=$(el $t0)
  s $R "$SUMS" > $OUT/r$i-after.txt
  f1=$(inf $M stats sync_full)
  same() { [ "$(grep " $1\$" $OUT/r$i-good.txt | cut -d' ' -f1)" = "$(grep " $1\$" $OUT/r$i-after.txt | cut -d' ' -f1)" ] && [ -n "$(grep " $1\$" $OUT/r$i-good.txt | cut -d' ' -f1)" ] && echo yes || echo NO; }
  says=$(s $R "awk '/=== round $i: SIGTERM mid sync/{f=1} f' /mnt/kv/server.log" | grep -c 'the final save is skipped')
  s $R "echo '=== round $i: start after the stop $(date -u +%FT%T.%3NZ)' >> /mnt/kv/server.log; $START"
  t1=$(date +%s.%N); in_step; back=$(el $t1)
  echo "$i,$d,$((f1 - f0)),${sip:-?}$([ -z "$seen" ] && echo " (sync never seen)"),${ld:-?},$ex,$(same dump.kdb),$(same dump.kdb.repl),$says,$back" | tee -a $OUT/rounds.csv
done
kill $SG 2>/dev/null; wait $SG 2>/dev/null
for _ in $(seq 1 300); do
  a=$(inf $M replication master_repl_offset); b=$(inf $R replication slave_repl_offset)
  [ -n "$a" ] && [ "$a" = "$b" ] && break; sleep 1
done
sleep 2
echo "end: offsets master=$a replica=$b; DBSIZE master $(redis-cli -h $M DBSIZE) replica $(redis-cli -h $R DBSIZE); master sync_full $(inf $M stats sync_full) sync_partial_ok $(inf $M stats sync_partial_ok)" | tee -a $OUT/result.txt
s $M 'cat /mnt/kv/server.log' > $OUT/server-master.log
s $R 'cat /mnt/kv/server.log' > $OUT/server-replica.log
s $M 'bash server.sh stop'; s $R 'bash server.sh stop'
echo R5-DONE
