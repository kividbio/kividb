#!/usr/bin/env bash
# ab_restart.sh: does a replica come back after a clean master restart under load? rc7, rc6 and Redis 8.10.3.
# Same pair as soak 1: master + replica, appendonly yes / everysec, defaults otherwise (Redis: io-threads 4,
# save "", and its own defaults, shutdown-timeout 10 and client-output-buffer-limit replica 256mb 64mb 60).
# Dataset of soak 1's size: 2,000,000 string keys prefilled (soakgen -str-keys 2000000) plus the soak mix. Then:
#   4 min of the rc7 soak mix at 100k ops/s, SIGTERM to the master (as soak 1 at 55 min), start it again,
#   and 6 more minutes of load. Polled every 0.1 s around the restart: the master's master_repl_offset (last seen
#   before it exits) and the replica's slave_repl_offset; then the master's sync_full / sync_partial_ok and the
#   replica's link, every 1 s.
set -u
. $HOME/hosts.env
OUT=$HOME/abr; mkdir -p $OUT
s() { for i in 1 2 3 4 5 6; do ssh -o StrictHostKeyChecking=no -o LogLevel=ERROR "$@"; rc=$?; [ $rc != 255 ] && return $rc; sleep 3; done; return 255; }
inf() { timeout 1 redis-cli -h $1 INFO $2 2>/dev/null | tr -d '\r' | awk -F: -v k=$3 '$1==k{print $2}'; }
run() { # label engine binary-path-for-restart
  local d=$OUT/$1; rm -rf $d; mkdir -p $d
  s $M "ENGINE=$2 bash server.sh start-master" > $d/start.txt
  s $R "ENGINE=$2 bash server.sh start-replica $M" >> $d/start.txt
  sleep 3
  redis-cli -h $M INFO server | tr -d '\r' | grep -E '^(kividb_version|redis_version):' > $d/version.txt
  $HOME/soakgen -addr $M:6379 -conns 64 -prefill -str-keys 2000000
  $HOME/soakgen -addr $M:6379 -conns 128 -rate 100000 -duration 10m -str-keys 2000000 > $d/soak.csv 2> $d/soakgen.err &
  local SG=$!
  sleep 240
  echo "keys before restart: master $(redis-cli -h $M DBSIZE) replica $(redis-cli -h $R DBSIZE)" > $d/result.txt
  # poll offsets fast while the master stops
  ( while :; do echo "$(date +%s.%N),$(inf $M replication master_repl_offset),$(inf $R replication slave_repl_offset),$(inf $R replication master_link_status)"; sleep 0.1; done ) > $d/offsets-fast.csv &
  local P=$!
  sleep 1
  local t=$(date +%s.%N)
  s $M 'p=$(cat /mnt/kv/pid); kill -TERM $p; for i in $(seq 1 1200); do kill -0 $p 2>/dev/null || break; sleep 0.05; done'
  local te=$(date +%s.%N)
  sleep 1
  kill $P
  local mlast=$(awk -F, '$2!=""{v=$2} END{print v}' $d/offsets-fast.csv)
  local rafter=$(inf $R replication slave_repl_offset)
  echo "master exited after $(awk -v a=$t -v b=$te 'BEGIN{printf "%.2f", b-a}') s; last master offset seen $mlast; replica offset after the exit $rafter; replica minus master: $((rafter - mlast)) bytes" >> $d/result.txt
  s $M 'echo "=== restart $(date -u +%FT%TZ)" >> /mnt/kv/server.log; (cd /mnt/kv && exec nohup $(head -1 /mnt/kv/cmd) >> /mnt/kv/server.log 2>&1 < /dev/null) & echo $! > /mnt/kv/pid'
  local ts=$(date +%s.%N)
  ( while :; do echo "$(date +%s.%N),$(inf $M stats sync_full),$(inf $M stats sync_partial_ok),$(inf $R replication master_link_status),$(inf $M replication connected_slaves),$(inf $M replication master_repl_offset),$(inf $R replication slave_repl_offset)"; sleep 1; done ) > $d/after.csv &
  P=$!
  wait $SG
  sleep 30
  kill $P
  # converged = link up and replica offset within 1 MB of the master's for the last 30 s
  echo "after the restart (1 s samples, $(wc -l < $d/after.csv)): final sync_full=$(inf $M stats sync_full) sync_partial_ok=$(inf $M stats sync_partial_ok); link-down samples=$(awk -F, '$4!="up"' $d/after.csv | wc -l); last link-down at $(awk -F, -v t0=$ts '$4!="up"{x=$1-t0} END{printf "%.0f s", x}' $d/after.csv) after the start" >> $d/result.txt
  s $M 'cat /mnt/kv/server.log' > $d/server-master.log
  s $R 'cat /mnt/kv/server.log' > $d/server-replica.log
  echo "master log: evictions=$(grep -c -i -E 'exceeded backlog limit|scheduled to be closed ASAP|output buffer' $d/server-master.log)" >> $d/result.txt
  cat $d/result.txt
}
run rc7 kividb
run rc6 kividbrc6
run redis redis
s $M 'bash server.sh stop'; s $R 'bash server.sh stop'
echo ABR-DONE
