#!/usr/bin/env bash
# ab_fullsync.sh: does a full sync converge under load? rc7, rc6, Redis 8.10.3 on soak 1's pair: KEYS (2,000,000) keys
# prefilled, the rc7 soak mix at RATE ops/s (100k, and 25k/50k/75k for rc7); after 3 min the replica is killed, its data wiped and it is started
# again (so it must full-sync); 4 more minutes of load. Counts the master's evictions of the replica and full syncs,
# when the replica's link was last down, the sync's duration (restart to the last 'Synced'/'Finished with success'),
# and the peak of master offset minus the replica's acknowledged offset (INFO slave0) as the peak of its queue.
set -u
. $HOME/hosts.env
OUT=${OUT:-$HOME/abfs}; KEYS=${KEYS:-2000000}; mkdir -p $OUT
s() { for i in 1 2 3 4 5 6; do ssh -o StrictHostKeyChecking=no -o LogLevel=ERROR "$@"; rc=$?; [ $rc != 255 ] && return $rc; sleep 3; done; return 255; }
inf() { timeout 1 redis-cli -h $1 INFO $2 2>/dev/null | tr -d '\r' | awk -F: -v k=$3 '$1==k{print $2}'; }
run() { # label engine rate
  local d=$OUT/$1; rm -rf $d; mkdir -p $d
  s $M "ENGINE=$2 bash server.sh start-master" > $d/start.txt
  s $R "ENGINE=$2 bash server.sh start-replica $M" >> $d/start.txt
  sleep 3
  $HOME/soakgen -addr $M:6379 -conns 64 -prefill -str-keys $KEYS
  $HOME/soakgen -addr $M:6379 -conns 128 -rate $3 -duration 7m -str-keys $KEYS > $d/soak.csv 2> $d/soakgen.err &
  local SG=$!
  sleep 180
  local full0=$(inf $M stats sync_full)
  s $R 'kill -9 $(cat /mnt/kv/pid); sleep 0.5; find /mnt/kv -maxdepth 1 -type f ! -name node.conf ! -name cmd ! -name server.log ! -name pid -delete; rm -rf /mnt/kv/appendonlydir; echo "=== wiped and restarted $(date -u +%FT%TZ)" >> /mnt/kv/server.log; (cd /mnt/kv && exec nohup $(head -1 /mnt/kv/cmd) >> /mnt/kv/server.log 2>&1 < /dev/null) & echo $! > /mnt/kv/pid'
  local ts=$(date +%s.%N)
  ( while :; do echo "$(date +%s.%N),$(inf $M stats sync_full),$(inf $R replication master_link_status),$(inf $M replication master_repl_offset),$(inf $R replication slave_repl_offset),$(timeout 1 redis-cli -h $M INFO replication | tr -d '\r' | grep -o 'offset=[0-9]*' | head -1 | cut -d= -f2)"; sleep 1; done ) > $d/after.csv &
  local P=$!
  wait $SG; sleep 20; kill $P
  s $M 'cat /mnt/kv/server.log' > $d/server-master.log
  s $R 'cat /mnt/kv/server.log' > $d/server-replica.log
  local synced=$(grep -E "Synced with|Finished with success" $d/server-replica.log | tail -1 | cut -c1-23)
  local dur=$(awk -v t0=$ts -v s="$synced" 'BEGIN{gsub(/[-T:Z]/," ",s); split(s,a," "); t=mktime(a[1]" "a[2]" "a[3]" "a[4]" "a[5]" "int(a[6]))+(a[6]-int(a[6])); printf "%.1f", t-t0}' 2>/dev/null)
  local peak=$(awk -F, '$4!="" && $6!=""{q=$4-$6; if(q>m)m=q} END{printf "%.0f", m/1048576}' $d/after.csv)
  echo "$1: rate $3; keys $(redis-cli -h $M DBSIZE); full syncs after the wipe $(( $(inf $M stats sync_full) - full0 )); evictions $(sed -n '/Channel replica\|client-output-buffer\|scheduled to be closed/p' $d/server-master.log | grep -c -i -E 'exceeded backlog|scheduled to be closed|output buffer'); link-down samples $(awk -F, '$3!="up"' $d/after.csv | wc -l); last link-down $(awk -F, -v t0=$ts '$3!="up"{x=$1-t0} END{printf "%.0f", x}' $d/after.csv) s after the restart (load ran $(awk -v t0=$ts -v e=$(date +%s) 'BEGIN{printf "%.0f", e-t0-20}') s after it); last sync done $dur s after the restart; peak queue ~${peak} MiB" | tee -a $OUT/summary.txt
}
if [ -n "${RUNS:-}" ]; then
  for x in $RUNS; do IFS=: read -r lab eng rate <<< "$x"; run $lab $eng $rate; done
else
  run rc7-100k kividb 100000
  run rc6-100k kividbrc6 100000
  run redis-100k redis 100000
  run rc7-75k kividb 75000
  run rc7-50k kividb 50000
  run rc7-25k kividb 25000
fi
s $M 'bash server.sh stop'; s $R 'bash server.sh stop'
echo ABFS-DONE $OUT
