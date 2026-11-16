#!/usr/bin/env bash
# expire_replay.sh <binary> <label>: a key whose PEXPIREAT is followed by another write in the AOF, killed before the
# expiry, restarted after it. Redis: the key is gone (it expired). Shown: what the restarted server holds.
B=$1; D=/tmp/er-$2; P=6390
[ -f $D/pid ] && kill -9 $(cat $D/pid) 2>/dev/null; rm -rf $D; mkdir -p $D; cd $D
printf "port $P\nbind 127.0.0.1\ndir $D\nappendonly yes\nappendfsync always\nupdate-check no\n" > node.conf
case $B in *redis-server) CMD="$B $D/node.conf"; sed -i '/update-check/d' node.conf ;; *) CMD="$B --configfile $D/node.conf" ;; esac   # Redis as the reference
run() { nohup $CMD >> $D/server.log 2>&1 < /dev/null & echo $! > $D/pid; for i in $(seq 1 50); do redis-cli -p $P PING 2>/dev/null | grep -q PONG && break; sleep 0.1; done; }
run
c() { redis-cli -p $P "$@"; }
c HSET h f0 v0 > /dev/null; c PEXPIRE h 2000 > /dev/null; c HSET h f1 v1 > /dev/null          # hash: TTL, then another field
c SET s v > /dev/null; c PEXPIRE s 2000 > /dev/null; c APPEND s x > /dev/null                 # string: TTL, then APPEND
c RPUSH l a > /dev/null; c PEXPIRE l 2000 > /dev/null; c RPUSH l b > /dev/null                # list
echo "$2 before kill: h ttl=$(c PTTL h) hlen=$(c HLEN h) | s ttl=$(c PTTL s) | l ttl=$(c PTTL l)"
sleep 0.3; kill -9 $(cat $D/pid); sleep 2.5      # killed before the expiry, restarted after it
run
echo "$2 after restart: h type=$(c TYPE h) ttl=$(c PTTL h) hlen=$(c HLEN h) fields=$(c HKEYS h | tr '\n' ',') | s type=$(c TYPE s) ttl=$(c PTTL s) val=$(c GET s) | l type=$(c TYPE l) ttl=$(c PTTL l) len=$(c LLEN l) | dbsize=$(c DBSIZE)"
kill -9 $(cat $D/pid)
