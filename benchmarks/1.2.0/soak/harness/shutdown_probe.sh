#!/usr/bin/env bash
# shutdown_probe.sh: why does the replica fall behind the master's saved point at a SIGTERM?
# For rc7 and rc6, with and without keys that expire: fresh pair (master `loglevel verbose`), 300 k keys prefilled,
# 60 s of the soak mix at 30k ops/s (ttl=no: -drop 2,14,16,17, so no key or field ever gets a time to live), the
# load stopped, 30 s idle, then SIGTERM to the master and a restart. A `redis-cli --replica` beside the real replica
# prints, with timestamps, every command the master streams. From the logs: the restarted master's saved point
# ("Loaded dataset is offset"), the replica's offset when its link dropped, partial or full resync.
set -u
. $HOME/hosts.env
OUT=$HOME/probe; mkdir -p $OUT
s() { for i in 1 2 3 4 5 6; do ssh -o StrictHostKeyChecking=no -o LogLevel=ERROR "$@"; rc=$?; [ $rc != 255 ] && return $rc; sleep 3; done; return 255; }
inf() { timeout 1 redis-cli -h $1 INFO $2 2>/dev/null | tr -d '\r' | awk -F: -v k=$3 '$1==k{print $2}'; }
run() { # label engine ttl
  local d=$OUT/$1; rm -rf $d; mkdir -p $d
  s $M "ENGINE=$2 EXTRA='loglevel verbose' bash server.sh start-master" > $d/start.txt
  ENGINE=$2 bash server.sh start-replica $M >> $d/start.txt
  sleep 2
  $HOME/soakgen -addr $M:6379 -conns 32 -prefill -str-keys 300000
  ( timeout 400 redis-cli -h $M --replica 2>&1 | while IFS= read -r l; do echo "$(date +%s.%N) $l"; done ) > $d/fake-replica.txt &
  local FR=$!
  local drop=""; [ $3 = no ] && drop="2,14,16,17"
  $HOME/soakgen -addr $M:6379 -conns 64 -rate 30000 -duration 60s -str-keys 300000 -drop "$drop" > $d/soak.csv 2> $d/soakgen.err
  sleep 30
  echo "before SIGTERM: master offset $(inf $M replication master_repl_offset) replica offset $(inf $R replication slave_repl_offset) expired_keys $(inf $M stats expired_keys) expires $(redis-cli -h $M INFO keyspace | tr -d '\r' | awk -F'[:=,]' '/^db0:/{print $5}')" > $d/result.txt
  ( for _ in $(seq 1 60); do echo "$(date +%s.%N) m=$(inf $M replication master_repl_offset) r=$(inf $R replication slave_repl_offset)"; sleep 0.1; done ) > $d/offsets.txt &
  sleep 1
  echo "$(date +%s.%N) SIGTERM" >> $d/offsets.txt
  s $M 'p=$(cat /mnt/kv/pid); kill -TERM $p; for i in $(seq 1 1200); do kill -0 $p 2>/dev/null || break; sleep 0.05; done'
  echo "$(date +%s.%N) exited" >> $d/offsets.txt
  sleep 1
  s $M 'echo "=== restart $(date -u +%FT%TZ)" >> /mnt/kv/server.log; (cd /mnt/kv && exec nohup $(head -1 /mnt/kv/cmd) >> /mnt/kv/server.log 2>&1 < /dev/null) & echo $! > /mnt/kv/pid'
  sleep 15
  kill $FR 2>/dev/null; pkill -f "redis-cli -h $M --replica"
  s $M 'cat /mnt/kv/server.log' > $d/server-master.log
  cp /mnt/kv/server.log $d/server-replica.log
  local loaded=$(grep -o "Loaded dataset is offset [0-9]*" $d/server-master.log | tail -1 | awk '{print $NF}')
  local ended=$(sed -n '/Replication link to .* failed/,$p' $d/server-replica.log | grep -o -E "it ended at [0-9]+|continuing from offset [0-9]+" | head -1 | awk '{print $NF}')
  echo "$1: loaded=$loaded replica_at_drop=$ended behind=$(( ${loaded:-0} - ${ended:-0} )) partial=$(sed -n '/=== restart/,$p' $d/server-master.log | grep -c 'Partial resync') full=$(sed -n '/=== restart/,$p' $d/server-master.log | grep -c 'sent full RDB')" | tee -a $d/result.txt $OUT/summary.txt
}
for r in 1 2; do
  run r$r-rc7-ttl kividb yes; run r$r-rc7-nottl kividb no
  run r$r-rc6-ttl kividbrc6 yes; run r$r-rc6-nottl kividbrc6 no
done
s $M 'bash server.sh stop'; bash server.sh stop
echo PROBE-DONE
