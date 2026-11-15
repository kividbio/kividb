#!/usr/bin/env bash
# hdigest_repro.sh <kividb-binary> : does DEBUG DIGEST-VALUE of a hash with field TTLs survive SAVE + restart,
# and is it stable without a restart? Prints the digests and HGETALL/HPEXPIRETIME before and after.
BIN=$1; P=7590; D=$(mktemp -d)
c() { redis-cli -p $P "$@"; }
start() { (cd $D && exec $BIN --port $P --dir $D --loglevel warning >> $D/log 2>&1) & for i in $(seq 50); do c PING >/dev/null 2>&1 && break; sleep 0.1; done; }
start
for k in 1 2 3 4 5 6; do c HSET h$k f0 a f1 b f2 c f3 d f4 e f5 f f6 g f7 h f8 i f9 j >/dev/null; done
c HPEXPIRE h1 3600000 FIELDS 1 f0 >/dev/null
c HPEXPIRE h2 3600000 FIELDS 3 f0 f3 f9 >/dev/null
c HPEXPIRE h3 3600000 FIELDS 1 f9 >/dev/null
c HPEXPIRE h4 3600000 FIELDS 10 f0 f1 f2 f3 f4 f5 f6 f7 f8 f9 >/dev/null
c HSET h5 x y >/dev/null
echo "before: $(for k in 1 2 3 4 5 6; do c DEBUG DIGEST-VALUE h$k | cut -c1-12; done | tr '\n' ' ')"
echo "again : $(for k in 1 2 3 4 5 6; do c DEBUG DIGEST-VALUE h$k | cut -c1-12; done | tr '\n' ' ')"
c HGETALL h2 | tr '\n' ' '; echo; c HPEXPIRETIME h2 FIELDS 3 f0 f3 f9 | tr '\n' ' '; echo
c SAVE >/dev/null; kill -9 %1; wait 2>/dev/null; start
echo "after : $(for k in 1 2 3 4 5 6; do c DEBUG DIGEST-VALUE h$k | cut -c1-12; done | tr '\n' ' ')"
c HGETALL h2 | tr '\n' ' '; echo; c HPEXPIRETIME h2 FIELDS 3 f0 f3 f9 | tr '\n' ' '; echo
c OBJECT ENCODING h2
kill -9 %1; wait 2>/dev/null; rm -rf $D
