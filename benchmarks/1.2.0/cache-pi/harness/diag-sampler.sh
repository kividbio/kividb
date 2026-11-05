#!/bin/bash
# diag-sampler.sh   (runs on a server VM under nohup) -- once a second: commands processed, connected
# clients, and the largest per-connection idle time from CLIENT LIST. Used to see whether the
# 30 s client timeouts seen with KiviDB 1.1.3 come from a global stall or from single connections
# that wait while others are served.
while true; do
  t=$(date +%s.%N | cut -c1-14)
  c=$(timeout 2 redis-cli -p 6379 info stats 2>/dev/null | grep '^total_commands_processed:' | cut -d: -f2 | tr -d '\r')
  l=$(timeout 2 redis-cli -p 6379 client list 2>/dev/null)
  n=$(echo "$l" | grep -c 'addr=')
  idle=$(echo "$l" | grep -o ' idle=[0-9]*' | cut -d= -f2 | sort -n | tail -1)
  echo "$t ${c:-NA} $n ${idle:-NA} $(cat ~/run/pid 2>/dev/null)"
  sleep 1
done
