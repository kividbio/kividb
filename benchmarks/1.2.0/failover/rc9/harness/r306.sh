#!/usr/bin/env bash
# r306.sh <engine> <rounds>: #306. Master + replica (t1, t2), AOF on. 1 M keys prefilled, then the rc7 soak mix at
# 100k ops/s (hash fields with 10-300 s TTLs, EXPIRE on collections) plus ~10k/s of SET ... PX 1500 (keys expiring
# all the time), for the whole run. After 60 s of load, <rounds> times: SIGTERM the master, start it again, wait for
# the replica's link, and record whether the master continued it (sync_partial_ok) or sent the dataset (sync_full);
# then 30 s more load. Output in ~/r306-<engine>/ (or $OUT). EXTRA: directives for both nodes, e.g. "repl-backlog-size 128mb".
set -u
. $HOME/hosts.env
E=$1 N=$2 OUT=${OUT:-$HOME/r306-$1}
rm -rf $OUT; mkdir -p $OUT
s() { ssh -o LogLevel=ERROR "$@"; }
inf() { timeout 2 redis-cli -h $1 INFO $2 2>/dev/null | tr -d '\r' | awk -F: -v k=$3 '$1==k{print $2}'; }
s $M "EXTRA=\"${EXTRA:-}\" ENGINE=$E bash server.sh start-master" > $OUT/start.txt
s $R "EXTRA=\"${EXTRA:-}\" ENGINE=$E bash server.sh start-replica $M" >> $OUT/start.txt
sleep 2
$HOME/soakgen -addr $M:6379 -conns 64 -prefill -str-keys 1000000
dur=$(( 60 + N * 60 + 60 ))
$HOME/soakgen -addr $M:6379 -conns 128 -rate 100000 -duration ${dur}s -str-keys 1000000 > $OUT/soak.csv 2> $OUT/soakgen.err &
SG=$!
( while kill -0 $SG 2>/dev/null; do redis-benchmark -h $M -q -c 1 -n 200000 -r 100000 SET exp:__rand_int__ v PX 1500 > /dev/null 2>&1; done ) &
EX=$!
sleep 60
echo "round,exit_s,link_up_s,sync_full,sync_partial_ok,result,master_says" > $OUT/rounds.csv
for i in $(seq 1 $N); do
  t0=$(date +%s.%N)
  s $M 'echo "=== round sigterm $(date -u +%FT%TZ)" >> /mnt/kv/server.log; p=$(cat /mnt/kv/pid); kill -TERM $p; for i in $(seq 1 1200); do kill -0 $p 2>/dev/null || break; sleep 0.05; done'
  t1=$(date +%s.%N)
  s $M 'echo "=== restart $(date -u +%FT%TZ)" >> /mnt/kv/server.log; (cd /mnt/kv && exec nohup $(head -1 /mnt/kv/cmd) >> /mnt/kv/server.log 2>&1 < /dev/null) & echo $! > /mnt/kv/pid'
  up=""
  for _ in $(seq 1 600); do
    [ "$(inf $R replication master_link_status)" = up ] && [ -n "$(inf $M stats sync_full)" ] && { up=$(date +%s.%N); break; }
    sleep 0.2
  done
  sleep 1
  full=$(inf $M stats sync_full); part=$(inf $M stats sync_partial_ok)
  res=full; [ "$part" = 1 ] && [ "$full" = 0 ] && res=partial
  says=$(s $M "awk '/=== round sigterm/{n=NR} {l[NR]=\$0} END{for(i=n;i<=NR;i++) print l[i]}' /mnt/kv/server.log" | grep -o -E "has the whole stream, to offset [0-9]+ \([0-9]+ ms\)|did not acknowledge the end of the stream|was still being sent the dataset" | head -1)
  echo "$i,$(awk -v a=$t0 -v b=$t1 'BEGIN{printf "%.2f", b-a}'),$(awk -v a=$t1 -v b=${up:-0} 'BEGIN{printf "%.1f", b-a}'),$full,$part,$res,${says:-none}" | tee -a $OUT/rounds.csv
  sleep 30
done
kill $SG $EX 2>/dev/null; wait $SG 2>/dev/null
pkill -f '[r]edis-benchmark' 2>/dev/null
s $M 'cat /mnt/kv/server.log' > $OUT/server-master.log
s $R 'cat /mnt/kv/server.log' > $OUT/server-replica.log
echo "$E: $(grep -c ',partial,' $OUT/rounds.csv) partial of $N; soakgen errors $(wc -l < $OUT/soakgen.err)" | tee $OUT/summary.txt
s $M 'bash server.sh stop'; s $R 'bash server.sh stop'
echo R306-DONE $E
