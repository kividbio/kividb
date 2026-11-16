#!/usr/bin/env bash
# fieldttl_drift.sh <kividb-binary> : HPEXPIRE (relative) on 200 hashes, then compare HPEXPIRETIME
# before and after (a) AOF replay after kill -9, (b) a BACKUP START..SEAL restore, (c) a replica.
BIN=$1; P=7591; D=$(mktemp -d); D2=$(mktemp -d); D3=$(mktemp -d)
c() { redis-cli -p $P "$@"; }
start() { local dir=$1 port=$2; shift 2; (cd $dir && exec $BIN --port $port --dir $dir --loglevel warning "$@" >> $dir/log 2>&1) & for i in $(seq 100); do redis-cli -p $port PING 2>/dev/null | grep -q PONG && break; sleep 0.1; done; }
snap() { for i in $(seq 200); do redis-cli -p $1 HPEXPIRETIME fh$i FIELDS 2 a b | tr '\n' ' '; echo; done; }
start $D $P --aof
redis-cli -p 7592 PING >/dev/null 2>&1
start $D3 7593 ; redis-cli -p 7593 REPLICAOF 127.0.0.1 $P >/dev/null
for i in $(seq 200); do c HSET fh$i a 1 b 2 c 3 >/dev/null; done
c BACKUP START >/dev/null; sleep 0.5
for i in $(seq 200); do c HPEXPIRE fh$i $((9000000 + i * 7)) FIELDS 1 a >/dev/null; c HEXPIRE fh$i 50000 FIELDS 1 b >/dev/null; done
sleep 1; c BACKUP SEAL >/dev/null
snap $P > $D/before
snap 7593 > $D/replica
mkdir -p $D2/bk; for f in $(c BACKUP LIST); do cp $f $D2/bk/; done
kill -9 %1; sleep 0.3; start $D $P --aof; snap $P > $D/after-aof
start $D2 7592 --preload-file aof:$D2/bk/appendonly.aof.manifest; snap 7592 > $D/after-backup
echo "AOF replay:    $(diff $D/before $D/after-aof | grep -c '^<') of 200 hashes differ"; diff $D/before $D/after-aof | head -4
echo "backup restore: $(diff $D/before $D/after-backup | grep -c '^<') of 200 hashes differ"; diff $D/before $D/after-backup | head -4
echo "replica:        $(diff $D/before $D/replica | grep -c '^<') of 200 hashes differ"; diff $D/before $D/replica | head -4
kill -9 $(jobs -p) 2>/dev/null; wait 2>/dev/null; rm -rf $D $D2 $D3
