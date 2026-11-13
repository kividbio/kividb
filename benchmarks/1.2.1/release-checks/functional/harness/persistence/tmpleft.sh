#!/usr/bin/env bash
# tmpleft.sh <kividb-binary> : is the snapshot temp file of a checkpoint cut by kill -9 ever removed?
BIN=$1; P=7620; D=$(mktemp -d -p /data)
c() { redis-cli -p $P "$@"; }
start() { (cd $D && exec $BIN --port $P --dir $D "$@" >> $D/log 2>&1) & for i in $(seq 300); do c PING 2>/dev/null | grep -q PONG && break; sleep 0.1; done; }
start --aof
c DEBUG POPULATE 3000000 k 64 > /dev/null || redis-benchmark -p $P -t set -n 3000000 -r 3000000 -d 64 -P 64 -q > /dev/null
echo "keys: $(c DBSIZE)"
c BGSAVE; sleep 0.3
echo "during BGSAVE: $(ls $D | tr '\n' ' ')"
kill -9 %1; wait 2>/dev/null
echo "after kill -9:  $(ls $D | tr '\n' ' ')"
start --aof; echo "after restart:  $(ls $D | tr '\n' ' ')"
c BGSAVE > /dev/null; for i in $(seq 300); do c INFO persistence | grep -q 'rdb_bgsave_in_progress:0' && break; sleep 0.2; done; sleep 0.5
echo "after a BGSAVE: $(ls $D | tr '\n' ' ')"
c SHUTDOWN > /dev/null 2>&1; wait 2>/dev/null
echo "after SHUTDOWN: $(ls $D | tr '\n' ' ')"
rm -rf $D
