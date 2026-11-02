#!/bin/bash
# server-run.sh <engine> <policy>   (runs on the server VM)
# Starts ~/bin/<engine> fresh on core 1 with the flags from kividbio/kividb#202 (KiviDB) or from the
# reporter's random-1g-1k document (Redis), with <policy> as maxmemory-policy, then samples
# /proc status and INFO every 30 s into ~/run/samples.log.
ENGINE=$1; POLICY=${2:-allkeys-lfu}
mkdir -p ~/run && cd ~/run
pkill -x server; while pgrep -x server >/dev/null || ss -ltn | grep -q ":6379 "; do sleep 1; done
rm -f dump.kdb dump.rdb appendonly.aof server.log samples.log; rm -rf appendonlydir
cp ~/bin/"$ENGINE" ./server
case $ENGINE in
redis-*)
  ARGS=(--protected-mode no --appendonly no --save "" --maxmemory 1073741824 --maxclients 24
        --maxmemory-policy "$POLICY" --port 6379)
  ;;
kividb-*)
  ARGS=(--port 6379 --threads 1 --maxmemory 1073741824 --maxmemory-policy "$POLICY"
        --loglevel nothing --update-check no --protected-mode no)
  ;;
*) echo "unknown engine $ENGINE"; exit 2 ;;
esac
taskset -c 1 ./server "${ARGS[@]}" > server.log 2>&1 &
PID=$!
echo $PID > pid
sleep 1
{
  echo "engine: $ENGINE"; cat ~/bin/"$ENGINE".info
  echo "launch: taskset -c 1 ./server ${ARGS[*]}"
  echo "cmdline:$(tr '\0' ' ' < /proc/$PID/cmdline)"
  taskset -pc $PID
  echo "kernel: $(uname -r)"; lscpu | grep -E 'Model name|^CPU\(s\)|L2|L3'; free -m | head -2
} > cmdline.txt
(
  while kill -0 $PID 2>/dev/null; do
    echo "=== $(date +%s) $(date -u +%T)"
    grep -E 'VmRSS|VmHWM|Threads' /proc/$PID/status
    redis-cli -p 6379 info memory | grep -E '^(used_memory|used_memory_rss|maxmemory|mem_fragmentation_ratio):'
    redis-cli -p 6379 info stats | grep -E '^(total_commands_processed|keyspace_hits|keyspace_misses|evicted_keys|instantaneous_ops_per_sec):'
    redis-cli -p 6379 dbsize
    echo "stat: $(cut -d' ' -f14,15 /proc/$PID/stat)"
    sleep 30
  done
) > samples.log 2>&1 &
for i in $(seq 1 60); do redis-cli -p 6379 ping 2>/dev/null | grep -q PONG && break; sleep 1; done
echo "started $ENGINE $POLICY pid $PID"
