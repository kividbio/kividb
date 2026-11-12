#!/usr/bin/env bash
# dv_probe2.sh <bin>: which hash-field-TTL histories give a replica a different DEBUG DIGEST-VALUE
B=${1:-/opt/kv/kividb}
stop() { for d in /tmp/t1 /tmp/t2; do [ -f $d/pid ] && kill -9 $(cat $d/pid) 2>/dev/null; done; }
stop; rm -rf /tmp/t1 /tmp/t2; mkdir -p /tmp/t1 /tmp/t2
printf "port 7500\ndir /tmp/t1\nupdate-check no\n" > /tmp/t1/c.conf
printf "port 7501\ndir /tmp/t2\nupdate-check no\nreplicaof 127.0.0.1 7500\n" > /tmp/t2/c.conf
for d in /tmp/t1 /tmp/t2; do (cd $d; exec $B --configfile $d/c.conf > $d/log 2>&1 < /dev/null) & echo $! > $d/pid; done
sleep 2
m() { redis-cli -p 7500 "$@" > /dev/null; }
m HSET k1 a 1 b 2 c x d keep; m HEXPIRE k1 7200 FIELDS 2 a b
m HSET k2 a 1 b 2 c x d keep; m HEXPIRE k2 7200 FIELDS 2 a b; m HINCRBY k2 b 1
m HSET k3 a 1 b 2 c x d keep; m HEXPIRE k3 7200 FIELDS 2 a b; m HSET k3 b 5
m HSET k4 a 1 b 2; m HPEXPIRE k4 1500 FIELDS 1 a
m HSET k5 a 1 b 2; m HPEXPIRE k5 1500 FIELDS 1 a; m HPEXPIRE k5 7200000 FIELDS 1 b
m HSET k6 a 1 b 2 c 3; m HEXPIRE k6 7200 FIELDS 2 a b; m HPERSIST k6 FIELDS 1 a
p=$(redis-cli -p 7500 PIPELINE 2>/dev/null)
printf 'HSET k7 a 1 b 2 c x d keep\r\nHEXPIRE k7 7200 FIELDS 2 a b\r\n' | redis-cli -p 7500 > /dev/null
sleep 3
for k in k1 k2 k3 k4 k5 k6 k7; do a=$(redis-cli -p 7500 DEBUG DIGEST-VALUE $k); b=$(redis-cli -p 7501 DEBUG DIGEST-VALUE $k); [ "$a" = "$b" ] && r=same || r=DIFFERENT
  echo "$k $r  fields m=[$(redis-cli -p 7500 HKEYS $k | tr '\n' ' ')] r=[$(redis-cli -p 7501 HKEYS $k | tr '\n' ' ')] hlen m=$(redis-cli -p 7500 HLEN $k) r=$(redis-cli -p 7501 HLEN $k) exp m=$(redis-cli -p 7500 HPEXPIRETIME $k FIELDS 2 a b | tr '\n' ' ') r=$(redis-cli -p 7501 HPEXPIRETIME $k FIELDS 2 a b | tr '\n' ' ')"; done
stop
