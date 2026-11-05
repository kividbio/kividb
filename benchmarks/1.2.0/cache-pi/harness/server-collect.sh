#!/bin/bash
# server-collect.sh <tag>   (runs on the server VM, after the client finished)
# Final server-side numbers: /proc status (VmHWM), INFO, CPU ticks used by the server, then stops it.
TAG=$1
cd ~/run
PID=$(cat pid)
{
  cat cmdline.txt
  echo "--- /proc status"
  grep -E 'VmPeak|VmHWM|VmRSS|RssAnon' /proc/$PID/status
  echo "--- cpu ticks (utime stime, CLK_TCK=$(getconf CLK_TCK)) and process age"
  echo "stat: $(cut -d' ' -f14,15 /proc/$PID/stat)"
  echo "age_s: $(ps -o etimes= -p $PID)"
  echo "--- INFO memory"
  redis-cli -p 6379 info memory
  echo "--- INFO stats"
  redis-cli -p 6379 info stats
  echo "--- INFO keyspace"
  redis-cli -p 6379 info keyspace
  redis-cli -p 6379 dbsize
  echo "--- server.log tail"
  tail -20 server.log
} > ~/run/final-$TAG.txt 2>&1
cp samples.log ~/run/samples-$TAG.log
pkill -x server
echo collected
