#!/usr/bin/env bash
# s1_events.sh <out>: soak 1's events. Lines "unix_ts name ..." go to <out>/events.
#   55 min: clean restart of the master (SIGTERM, wait for exit, start with the same node.conf)
#   85 min: kill -9 of the replica, start again 20 s later (same node.conf, so it resyncs from the master)
set -u
OUT=$1; . $HOME/hosts.env
EV=$OUT/events
T1=${T1:-55} T2=${T2:-85}
s() { for i in 1 2 3 4 5 6; do ssh -o StrictHostKeyChecking=no -o LogLevel=ERROR "$@"; rc=$?; [ $rc != 255 ] && return $rc; sleep 3; done; return 255; }
ev() { echo "$(date +%s.%N) $*" >> $EV; echo "$(date -u +%T) $*"; }
el() { awk -v a=$1 -v b=$(date +%s.%N) 'BEGIN{printf "%.2f", b-a}'; }
info() { timeout 3 redis-cli -h $1 INFO $2 2>/dev/null | tr -d '\r' | awk -F: -v k=$3 '$1==k{print $2}'; }
until [ -f $OUT/soak.start ]; do sleep 1; done
T0=$(cut -d. -f1 $OUT/soak.start)
at() { while [ $(date +%s) -lt $((T0 + $1 * 60)) ]; do sleep 1; done; }
START='echo "=== restart $(date -u +%FT%TZ)" >> /mnt/kv/server.log; (cd /mnt/kv && exec nohup /opt/kv/kividb --configfile /mnt/kv/node.conf >> /mnt/kv/server.log 2>&1 < /dev/null) & echo $! > /mnt/kv/pid'
wait_serving() { # host
  local t=$(date +%s.%N)
  for _ in $(seq 1 1200); do [ "$(timeout 1 redis-cli -h $1 PING 2>/dev/null)" = PONG ] && [ "$(info $1 persistence loading)" = 0 ] && break; sleep 0.1; done
  el $t
}
wait_link() {
  local t=$(date +%s.%N)
  for _ in $(seq 1 3000); do [ "$(info $R replication master_link_status)" = up ] && break; sleep 0.2; done
  el $t
}
at $T1
ev "master-restart-start sigterm $M keys=$(timeout 2 redis-cli -h $M DBSIZE) sync_full=$(info $M stats sync_full) sync_partial_ok=$(info $M stats sync_partial_ok)"
t=$(date +%s.%N)
s $M 'p=$(cat /mnt/kv/pid); kill -TERM $p; for i in $(seq 1 1200); do kill -0 $p 2>/dev/null || break; sleep 0.05; done; kill -0 $p 2>/dev/null && echo STILL-RUNNING'
ev "master-restart-exited after $(el $t) s"
s $M "$START"
ev "master-restart-started"
ev "master-restart-serving after $(wait_serving $M) s keys=$(timeout 2 redis-cli -h $M DBSIZE)"
ev "master-restart-replica-linked after $(wait_link) s sync_full=$(info $M stats sync_full) sync_partial_ok=$(info $M stats sync_partial_ok)"
at $T2
ev "replica-kill9-start $R keys=$(timeout 2 redis-cli -h $R DBSIZE)"
s $R 'kill -9 $(cat /mnt/kv/pid)'
ev "replica-kill9-killed"
sleep 20
s $R "$START"
ev "replica-kill9-started"
ev "replica-kill9-serving after $(wait_serving $R) s"
ev "replica-kill9-linked after $(wait_link) s sync_full=$(info $M stats sync_full) sync_partial_ok=$(info $M stats sync_partial_ok) keys=$(timeout 2 redis-cli -h $R DBSIZE)"
ev "events-done"
