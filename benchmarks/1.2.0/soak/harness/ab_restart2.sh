#!/usr/bin/env bash
# ab_restart2.sh: repeated SIGTERM restarts of the master under load, rc7 and rc6 alternated, 3 rounds. Each round:
# fresh pair, 1 M keys prefilled, 90 s of the rc7 soak mix at 100k ops/s, SIGTERM, start again, 60 s more load.
# Reports, from the logs: the restarted master's "Loaded dataset is offset", the replica's offset when its link
# dropped ("it ended at" on its next sync, or the partial-resync offset), and whether the resync was partial or full.
set -u
. $HOME/hosts.env
OUT=$HOME/abr2; mkdir -p $OUT
s() { for i in 1 2 3 4 5 6; do ssh -o StrictHostKeyChecking=no -o LogLevel=ERROR "$@"; rc=$?; [ $rc != 255 ] && return $rc; sleep 3; done; return 255; }
inf() { timeout 1 redis-cli -h $1 INFO $2 2>/dev/null | tr -d '\r' | awk -F: -v k=$3 '$1==k{print $2}'; }
run() { # label engine
  local d=$OUT/$1; rm -rf $d; mkdir -p $d
  s $M "ENGINE=$2 bash server.sh start-master" > $d/start.txt
  s $R "ENGINE=$2 bash server.sh start-replica $M" >> $d/start.txt
  sleep 3
  $HOME/soakgen -addr $M:6379 -conns 64 -prefill
  $HOME/soakgen -addr $M:6379 -conns 128 -rate 100000 -duration 150s > $d/soak.csv 2> $d/soakgen.err &
  local SG=$!
  sleep 90
  s $M 'p=$(cat /mnt/kv/pid); kill -TERM $p; for i in $(seq 1 1200); do kill -0 $p 2>/dev/null || break; sleep 0.05; done'
  s $M 'echo "=== restart $(date -u +%FT%TZ)" >> /mnt/kv/server.log; (cd /mnt/kv && exec nohup $(head -1 /mnt/kv/cmd) >> /mnt/kv/server.log 2>&1 < /dev/null) & echo $! > /mnt/kv/pid'
  wait $SG; sleep 5
  s $M 'cat /mnt/kv/server.log' > $d/server-master.log
  s $R 'cat /mnt/kv/server.log' > $d/server-replica.log
  local loaded=$(grep -o "Loaded dataset is offset [0-9]*" $d/server-master.log | tail -1 | awk '{print $NF}')
  local ended=$(sed -n '/Replication link to .* failed/,$p' $d/server-replica.log | grep -o -E "it ended at [0-9]+|continuing from offset [0-9]+" | head -1 | awk '{print $NF}')
  echo "$1: loaded=$loaded replica_at_drop=$ended behind=$((loaded - ended)) partial=$(sed -n '/=== restart/,$p' $d/server-master.log | grep -c 'Partial resync') full=$(sed -n '/=== restart/,$p' $d/server-master.log | grep -c 'sent full RDB') sync_full=$(inf $M stats sync_full) sync_partial_ok=$(inf $M stats sync_partial_ok)" | tee -a $OUT/summary.txt
}
for r in 1 2 3; do run r$r-rc7 kividb; run r$r-rc6 kividbrc6; done
s $M 'bash server.sh stop'; s $R 'bash server.sh stop'
echo ABR2-DONE
