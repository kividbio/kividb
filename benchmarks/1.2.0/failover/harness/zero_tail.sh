#!/usr/bin/env bash
# zero_tail.sh <binary> <label>: what a power loss on ext4 can leave, made on purpose.
#   a) whole records, then 400 zero bytes            -> must start with every key
#   b) the last record cut in half, then 400 zeros    -> must start without that one key
#   c) 400 zeros, then a whole record after them      -> damage, not a torn tail: must refuse to start
B=$1; L=$2; P=6393; N=5000
one() { # case
  local D=/tmp/zt-$L-$1
  rm -rf $D; mkdir -p $D; cd $D
  printf "port $P\nbind 127.0.0.1\ndir $D\nappendonly yes\nupdate-check no\n" > node.conf
  nohup $B --configfile $D/node.conf >> server.log 2>&1 < /dev/null & echo $! > pid
  for i in $(seq 1 50); do redis-cli -p $P PING 2>/dev/null | grep -q PONG && break; sleep 0.1; done
  for i in $(seq 1 $N); do echo "SET k$i v$i"; done | redis-cli -p $P > /dev/null
  sleep 1.5; kill -9 $(cat pid); sleep 0.3
  f=$D/appendonly.aof; sz=$(stat -c %s $f)
  case $1 in
    a) head -c 400 /dev/zero >> $f ;;
    b) last=$(printf '*3\r\n$3\r\nSET\r\n$5\r\nk%d\r\n$5\r\nv%d\r\n' $N $N | wc -c); truncate -s $((sz - last / 2)) $f; head -c 400 /dev/zero >> $f ;;
    c) head -c 400 /dev/zero >> $f; printf '*3\r\n$3\r\nSET\r\n$5\r\nafter\r\n$1\r\nx\r\n' >> $f ;;
  esac
  nohup $B --configfile $D/node.conf >> server.log 2>&1 < /dev/null & echo $! > pid
  local up=no
  for i in $(seq 1 50); do redis-cli -p $P PING 2>/dev/null | grep -q PONG && { up=yes; break; }; sleep 0.1; done
  echo "$L case $1: aof $sz bytes before damage; started=$up dbsize=$(redis-cli -p $P DBSIZE 2>/dev/null) k$N=$(redis-cli -p $P GET k$N 2>/dev/null) after=$(redis-cli -p $P EXISTS after 2>/dev/null) | $(grep -a -E 'WARN|FATAL|ERROR' server.log | tail -2 | cut -c1-220 | tr '\n' ' ')"
  kill -9 $(cat pid) 2>/dev/null; sleep 0.3
}
one a; one b; one c
