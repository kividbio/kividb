#!/usr/bin/env bash
# expire_more.sh <binary> <label>: every EXPIRE form, followed by a write that keeps the TTL, killed before the
# deadline and restarted after it; then a second restart from the AOF the first restart wrote.
# Each of x1..x4 must be gone after both restarts (Redis: gone); `keep` (a deadline an hour ahead) must keep its
# TTL and both elements. Hash fields: g (HPEXPIRE + HINCRBY) and q (HPEXPIREAT + HINCRBYFLOAT) must be gone,
# the hash `hq` with its persistent field `keep` must stay.
B=$1; D=/tmp/em-$2; P=6392
[ -f $D/pid ] && kill -9 $(cat $D/pid) 2>/dev/null; rm -rf $D; mkdir -p $D; cd $D
printf "port $P\nbind 127.0.0.1\ndir $D\nappendonly yes\nappendfsync always\nupdate-check no\n" > node.conf
case $B in *redis-server) CMD="$B $D/node.conf"; sed -i '/update-check/d' node.conf ;; *) CMD="$B --configfile $D/node.conf" ;; esac   # Redis as the reference
run() { nohup $CMD >> $D/server.log 2>&1 < /dev/null & echo $! > $D/pid; for i in $(seq 1 50); do redis-cli -p $P PING 2>/dev/null | grep -q PONG && break; sleep 0.1; done; }
c() { redis-cli -p $P "$@"; }
run
now_ms=$(c TIME | head -1)000
dl_ms=$((now_ms + 2000)); dl_s=$(( (now_ms + 2999) / 1000 ))
c HSET x1 a 1 > /dev/null; c EXPIRE x1 2 > /dev/null;              c HSET x1 b 2 > /dev/null      # hash, EXPIRE (s)
c SET x2 v > /dev/null;    c PEXPIREAT x2 $dl_ms > /dev/null;      c APPEND x2 x > /dev/null      # string, PEXPIREAT
c ZADD x3 1 a > /dev/null; c EXPIREAT x3 $dl_s > /dev/null;        c ZADD x3 2 b > /dev/null      # zset, EXPIREAT
c SADD x4 a > /dev/null;   c PEXPIRE x4 2000 > /dev/null;          c SADD x4 b > /dev/null        # set, PEXPIRE
c RPUSH keep a > /dev/null; c EXPIRE keep 3600 > /dev/null;        c RPUSH keep b > /dev/null     # control: deadline ahead
c HSET hq keep 1 g 5 q 1.5 > /dev/null; c HPEXPIRE hq 2000 FIELDS 1 g > /dev/null; c HINCRBY hq g 1 > /dev/null
c HPEXPIREAT hq $dl_ms FIELDS 1 q > /dev/null; c HINCRBYFLOAT hq q 1 > /dev/null
st() { echo "x1=$(c EXISTS x1) x2=$(c EXISTS x2) x3=$(c EXISTS x3) x4=$(c EXISTS x4) keep_ttl_s=$(c TTL keep) keep_len=$(c LLEN keep) hq_fields=$(c HKEYS hq | sort | tr '\n' ',') dbsize=$(c DBSIZE)"; }
echo "$2 before kill:     $(st)"
sleep 0.3; kill -9 $(cat $D/pid); sleep 3
run; echo "$2 after restart 1: $(st)"
sleep 1; c SET marker 1 > /dev/null; sleep 1.5          # let the expiry records reach the AOF (appendfsync always)
kill -9 $(cat $D/pid); sleep 0.5
run; echo "$2 after restart 2: $(st)"
echo "$2 deletion records in the AOF: $(cat $D/appendonly.aof* 2>/dev/null | tr -d '\r' | grep -a -c -E '^(UNLINK|DEL)$')"
kill -9 $(cat $D/pid)
