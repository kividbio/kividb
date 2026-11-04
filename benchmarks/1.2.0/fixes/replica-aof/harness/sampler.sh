#!/usr/bin/env bash
# sampler.sh <port> <pid-file> <out.csv> [interval_s]
# Every interval (10 s): process RSS and CPU, INFO memory / replication / persistence / keyspace of the server on this host.
set -u
PORT=$1 PIDF=$2 OUT=$3 IV=${4:-10}
echo "ts,rss_kb,cpu_pct,used_memory,used_memory_rss,keys,expires,master_repl_offset,replica_offset,replica_lag_bytes,role,aof_size,aof_acks_before_write,aof_rewrites,connected_slaves,sync_full,sync_partial_ok" > "$OUT"
f() { echo "$INFO" | awk -F: -v k="$1" '$1==k{print $2; exit}'; }
prev_cpu=0 prev_t=0
HZ=$(getconf CLK_TCK)
while :; do
  pid=$(cat "$PIDF" 2>/dev/null)
  now=$(date +%s.%N)
  rss=$(awk '/VmRSS/{print $2}' /proc/$pid/status 2>/dev/null)
  cpu_ticks=$(awk '{print $14+$15}' /proc/$pid/stat 2>/dev/null)
  cpu=""
  if [ -n "$cpu_ticks" ] && [ "$prev_t" != 0 ]; then
    cpu=$(echo "$cpu_ticks $prev_cpu $now $prev_t $HZ" | awk '{printf "%.0f", ($1-$2)/$5/($3-$4)*100}')
  fi
  prev_cpu=${cpu_ticks:-0}; prev_t=$now
  INFO=$(timeout 5 redis-cli -p "$PORT" INFO all 2>/dev/null | tr -d '\r')
  ks=$(echo "$INFO" | awk -F'[:=,]' '/^db0:/{print $3","$5}')
  moff=$(f master_repl_offset)
  role=$(f role)
  if [ "$role" = master ]; then
    roff=$(echo "$INFO" | awk -F'[:,=]' '/^slave0:/{for(i=1;i<=NF;i++) if($i=="offset") print $(i+1)}')
  else
    roff=$(f slave_repl_offset)
  fi
  lag=""
  [ -n "$moff" ] && [ -n "$roff" ] && lag=$((moff - roff))
  echo "$now,$rss,$cpu,$(f used_memory),$(f used_memory_rss),${ks:-,},$moff,$roff,$lag,$role,$(f aof_current_size),$(f aof_acks_before_write),$(f aof_rewrites),$(f connected_slaves),$(f sync_full),$(f sync_partial_ok)" >> "$OUT"
  sleep "$IV"
done
