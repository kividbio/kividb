#!/usr/bin/env bash
# field_replay.sh <binary> <label>: hash-field TTLs, killed before the field's deadline, restarted after it.
# f: HPEXPIRE then HSET of the same field (HSET clears a field's TTL: the field stays, as on the master).
# g: HPEXPIRE then HINCRBY of the field (keeps the field's TTL: on the master the field goes at its time).
B=$1; D=/tmp/fr-$2; P=6391
[ -f $D/pid ] && kill -9 $(cat $D/pid) 2>/dev/null; rm -rf $D; mkdir -p $D; cd $D
printf "port $P\nbind 127.0.0.1\ndir $D\nappendonly yes\nappendfsync always\nupdate-check no\n" > node.conf
case $B in *redis-server) CMD="$B $D/node.conf"; sed -i '/update-check/d' node.conf ;; *) CMD="$B --configfile $D/node.conf" ;; esac   # Redis as the reference
run() { nohup $CMD >> $D/server.log 2>&1 < /dev/null & echo $! > $D/pid; for i in $(seq 1 50); do redis-cli -p $P PING 2>/dev/null | grep -q PONG && break; sleep 0.1; done; }
run
c() { redis-cli -p $P "$@"; }
c HSET hf keep 1 f v0 > /dev/null; c HPEXPIRE hf 2000 FIELDS 1 f > /dev/null; c HSET hf f v1 > /dev/null
c HSET hg keep 1 g 5 > /dev/null; c HPEXPIRE hg 2000 FIELDS 1 g > /dev/null; c HINCRBY hg g 1 > /dev/null
echo "$2 before kill: hf f=$(c HGET hf f) fttl=$(c HPTTL hf FIELDS 1 f | tr '\n' ' ') | hg g=$(c HGET hg g) gttl=$(c HPTTL hg FIELDS 1 g | tr '\n' ' ')"
sleep 0.3; kill -9 $(cat $D/pid); sleep 2.5
run
echo "$2 after restart: hf f=$(c HGET hf f) fttl=$(c HPTTL hf FIELDS 1 f | tr '\n' ' ') | hg g=$(c HGET hg g) gttl=$(c HPTTL hg FIELDS 1 g | tr '\n' ' ') hlen=$(c HLEN hg)"
echo "--- AOF:"; cat $D/appendonly.aof* 2>/dev/null | tr -d '\r' | tr '\n' ' ' | sed 's/\*/\n*/g' | grep -a -v '^$' | head -30
kill -9 $(cat $D/pid)
