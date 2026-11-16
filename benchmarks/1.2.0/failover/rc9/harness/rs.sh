#!/usr/bin/env bash
# rs.sh <engine> <rounds>: a clean stop (SIGTERM) of a replica that is streaming under writes. Found by item 5's first
# run: the replica never exited. Master + replica (server.sh, ENGINE=kividb is rc9, rc8 is 082dab3d), AOF on,
# 1 M keys, the rc7 soak mix at 100k ops/s. <rounds> times: replica in step, SIGTERM, wait up to 30 s for the exit;
# if it is still running, its threads' stacks (gdb, first time) and kill -9; start it again, wait until in step.
# Then one control round with the load stopped. Output in ~/rs-<engine>/ (or $OUT).
set -u
. $HOME/hosts.env
E=$1 N=$2 OUT=${OUT:-$HOME/rs-$1}
rm -rf $OUT; mkdir -p $OUT
s() { ssh -o LogLevel=ERROR "$@"; }
inf() { timeout 2 redis-cli -h $1 INFO $2 2>/dev/null | tr -d '\r' | awk -F: -v k=$3 '$1==k{print $2}'; }
el() { awk -v a=$1 -v b=$(date +%s.%N) 'BEGIN{printf "%.2f", b-a}'; }
in_step() { for _ in $(seq 1 1200); do [ "$(inf $R replication master_link_status)" = up ] && [ "$(inf $R replication master_sync_in_progress)" = 0 ] && return 0; sleep 0.5; done; return 1; }
STOP='p=$(cat /mnt/kv/pid); kill -TERM $p; for i in $(seq 1 600); do kill -0 $p 2>/dev/null || { echo exited; exit 0; }; sleep 0.05; done; echo HUNG $p'
START='(cd /mnt/kv && exec nohup $(head -1 /mnt/kv/cmd) >> /mnt/kv/server.log 2>&1 < /dev/null) & echo $! > /mnt/kv/pid'
s $M "ENGINE=$E bash server.sh start-master" > $OUT/start.txt
s $R "ENGINE=$E bash server.sh start-replica $M" >> $OUT/start.txt
s $R "head -1 /mnt/kv/cmd; \$(head -1 /mnt/kv/cmd | cut -d' ' -f1) --version" >> $OUT/start.txt
sleep 2
$HOME/soakgen -addr $M:6379 -conns 64 -prefill -str-keys 1000000
in_step
$HOME/soakgen -addr $M:6379 -conns 128 -rate 100000 -duration 60m -str-keys 1000000 > $OUT/soak.csv 2> $OUT/soakgen.err &
SG=$!
sleep 20
echo "round,load,result,exit_s,last_log_line" > $OUT/rounds.csv
round() { # i load
  in_step; sleep 5
  s $R "echo '=== round $1: SIGTERM ($2) $(date -u +%FT%T.%3NZ)' >> /mnt/kv/server.log"
  local t0=$(date +%s.%N)
  local r=$(s $R "$STOP")
  local ex=$(el $t0)
  local last=$(s $R "tail -1 /mnt/kv/server.log" | cut -c1-160 | tr ',' ';')
  if [ "${r%% *}" = HUNG ]; then
    [ -f $OUT/stacks.txt ] || s $R "sudo gdb -p ${r#HUNG } -batch -ex 'thread apply all bt 25' 2>&1" > $OUT/stacks.txt
    s $R "ss -tnp | grep kividb" > $OUT/sockets-r$1.txt
    s $R "kill -9 ${r#HUNG }; sleep 1"
  fi
  echo "$1,$2,${r%% *},$ex,$last" | tee -a $OUT/rounds.csv
  s $R "echo '=== round $1: start again $(date -u +%FT%T.%3NZ)' >> /mnt/kv/server.log; $START"
}
for i in $(seq 1 $N); do round $i "under load"; done
kill $SG 2>/dev/null; wait $SG 2>/dev/null
sleep 3
round $((N + 1)) "load stopped"
in_step
s $M 'cat /mnt/kv/server.log' > $OUT/server-master.log
s $R 'cat /mnt/kv/server.log' > $OUT/server-replica.log
echo "$E: $(grep -c ',under load,HUNG,' $OUT/rounds.csv) of $N clean stops under load hung; control (load stopped): $(tail -1 $OUT/rounds.csv | cut -d, -f3)" | tee $OUT/summary.txt
s $M 'bash server.sh stop'; s $R 'bash server.sh stop'
echo RS-DONE $E
