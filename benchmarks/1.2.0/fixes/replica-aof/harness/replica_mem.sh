#!/usr/bin/env bash
# replica_mem.sh <master-ip> <replica-ip> <out-dir> <name> <engine> [replica extra directives] [seconds] [rate]
# The soak's mix against a fresh master + replica; every 10 s the master's and the replica's used_memory,
# RSS (INFO) and the replica's checkpoint state. Shows whether the replica's memory tracks the master's.
set -u
M=$1 R=$2 OUT=$3 NAME=$4 ENGINE=$5 RX=${6:-} SECS=${7:-300} RATE=${8:-100000}
mkdir -p "$OUT"
s() { ssh -o StrictHostKeyChecking=no -o LogLevel=ERROR "$@"; }
s "$M" "ENGINE=$ENGINE bash server.sh start-master" >/dev/null
s "$R" "ENGINE=$ENGINE EXTRA='$RX' bash server.sh start-replica $M" >/dev/null
sleep 3
"$HOME/soakgen" -addr "$M:6379" -conns 64 -prefill
f() { redis-cli -h "$1" -p 6379 INFO "$2" | tr -d '\r' | awk -F: -v k="$3" '$1==k{print $2}'; }
echo "t_s,master_used,master_rss,replica_used,replica_rss,replica_link,replica_aof_rewrite_in_progress" > "$OUT/$NAME.csv"
"$HOME/soakgen" -addr "$M:6379" -conns 128 -rate "$RATE" -duration "${SECS}s" -interval 10s > "$OUT/$NAME.load.csv" 2>/dev/null &
G=$!
t0=$(date +%s)
while kill -0 $G 2>/dev/null; do
  echo "$(( $(date +%s) - t0 )),$(f $M memory used_memory),$(f $M memory used_memory_rss),$(f $R memory used_memory),$(f $R memory used_memory_rss),$(f $R replication master_link_status),$(f $R persistence aof_rewrite_in_progress)" >> "$OUT/$NAME.csv"
  sleep 10
done
s "$R" "grep -E 'auto-checkpoint|rebased onto|Replication' /mnt/kv/server.log | tail -20" > "$OUT/$NAME.replica-log.txt"
s "$M" "grep -E 'auto-checkpoint|rebased onto' /mnt/kv/server.log | tail -5" > "$OUT/$NAME.master-log.txt"
tail -1 "$OUT/$NAME.csv"
