#!/usr/bin/env bash
# dv_probe.sh <bin>: DEBUG DIGEST-VALUE of equal values on a master and its replica (same version, one host)
B=${1:-/opt/kv/kividb}
stop() { for d in /tmp/t1 /tmp/t2; do [ -f $d/pid ] && kill -9 $(cat $d/pid) 2>/dev/null; done; }
stop; rm -rf /tmp/t1 /tmp/t2; mkdir -p /tmp/t1 /tmp/t2
printf "port 7500\ndir /tmp/t1\nupdate-check no\n" > /tmp/t1/c.conf
printf "port 7501\ndir /tmp/t2\nupdate-check no\nreplicaof 127.0.0.1 7500\n" > /tmp/t2/c.conf
for d in /tmp/t1 /tmp/t2; do (cd $d; exec $B --configfile $d/c.conf > $d/log 2>&1 < /dev/null) & echo $! > $d/pid; done
sleep 2
m() { redis-cli -p 7500 "$@" > /dev/null; }
m HSET h1 a 1 b 2 c 3
m HSET h2 a 1 b 2 c 3; m HPEXPIREAT h2 4102444800000 FIELDS 1 a
m HSET h3 a 1 b 2 c 3; m HEXPIRE h3 3600 FIELDS 1 a
m XADD x1 1-1 f v; m XADD x1 2-1 f v
m XADD x2 1-1 f v; m XGROUP CREATE x2 g 0
m XADD x3 1-1 f v; m XGROUP CREATE x3 g 0; m XREADGROUP GROUP g c COUNT 1 STREAMS x3 '>'
m JSON.SET j1 '$' '{"a":1}' ; m SET s1 v; m SETEX s2 100 v
sleep 3
for k in h1 h2 h3 x1 x2 x3 j1 s1 s2; do a=$(redis-cli -p 7500 DEBUG DIGEST-VALUE $k); b=$(redis-cli -p 7501 DEBUG DIGEST-VALUE $k); [ "$a" = "$b" ] && r=same || r=DIFFERENT; echo "$k $(redis-cli -p 7500 TYPE $k) $r"; done
echo "h2 HPEXPIRETIME master $(redis-cli -p 7500 HPEXPIRETIME h2 FIELDS 1 a) replica $(redis-cli -p 7501 HPEXPIRETIME h2 FIELDS 1 a)"
# the same value twice on one node
m HSET h4 a 1 b 2 c 3; m HPEXPIREAT h4 4102444800000 FIELDS 1 a
echo "h2 vs h4 on the master: $( [ "$(redis-cli -p 7500 DEBUG DIGEST-VALUE h2)" = "$(redis-cli -p 7500 DEBUG DIGEST-VALUE h4)" ] && echo same || echo DIFFERENT)"
stop
