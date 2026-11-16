#!/usr/bin/env bash
# pair.sh <kividb binary> | stop : a master :6501 and its replica :6502 on this host (no AOF, no maxmemory), for fexp_race.py
set -u
for p in 6501 6502; do redis-cli -p $p SHUTDOWN NOSAVE > /dev/null 2>&1; done; sleep 1
[ "$1" = stop ] && exit 0
B=$1
rm -rf /tmp/pr; mkdir -p /tmp/pr/m /tmp/pr/r
printf 'port 6501\nbind 0.0.0.0\nprotected-mode no\ndir /tmp/pr/m\nupdate-check no\n' > /tmp/pr/m/node.conf
printf 'port 6502\nbind 0.0.0.0\nprotected-mode no\ndir /tmp/pr/r\nupdate-check no\nreplicaof 127.0.0.1 6501\n' > /tmp/pr/r/node.conf
(cd /tmp/pr/m && exec nohup $B --configfile /tmp/pr/m/node.conf > log 2>&1 < /dev/null) &
sleep 1
(cd /tmp/pr/r && exec nohup $B --configfile /tmp/pr/r/node.conf > log 2>&1 < /dev/null) &
for _ in $(seq 1 50); do redis-cli -p 6502 INFO replication 2>/dev/null | grep -q master_link_status:up && break; sleep 0.2; done
echo "pair up: $($B --version) $(redis-cli -p 6502 INFO replication | tr -d '\r' | grep master_link_status)"
