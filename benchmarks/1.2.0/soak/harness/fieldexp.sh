#!/usr/bin/env bash
# fieldexp.sh <kividb binary> <label>: does a hash whose every field expires go away, after a restart?
# For each case: a fresh server (appendonly yes, appendfsync always), three hashes
#   h1: one field, HPEXPIRE 3000                -> should vanish
#   h2: two fields, both HPEXPIRE 3000           -> should vanish
#   h3: two fields, one HPEXPIRE 3000 (control)  -> should stay, HLEN 1
# then: none   (no restart)
#       term-early / kill9-early  (stop after 0.5 s, start again at once: the deadlines are still ahead at load)
#       term-late / kill9-late    (stop after 0.5 s, start again after 4 s: the deadlines passed while it was down)
# and 5 s after the deadlines: EXISTS, HLEN, TYPE of each, DBSIZE. Redis: h1 and h2 gone, DBSIZE 1.
set -u
B=$1; LBL=$2; P=6500; D=/tmp/fx
cli() { redis-cli -p $P "$@"; }
srvpid() { cli INFO server 2>/dev/null | tr -d '\r' | awk -F: '$1=="process_id"{print $2}'; }
stop9() { p=$(srvpid); [ -n "$p" ] && kill -9 $p; while [ -n "$(srvpid)" ]; do sleep 0.05; done; }
start() { printf 'port %s\nbind 127.0.0.1\ndir %s\nappendonly yes\nappendfsync always\nupdate-check no\n' $P $D > $D/node.conf
  (cd $D && exec nohup $B --configfile $D/node.conf >> $D/server.log 2>&1 < /dev/null) &
  for _ in $(seq 1 100); do [ "$(cli PING 2>/dev/null)" = PONG ] && [ "$(cli INFO persistence | tr -d '\r' | awk -F: '$1=="loading"{print $2}')" = 0 ] && break; sleep 0.1; done
  echo "    started pid $(srvpid) $(cli INFO server | tr -d '\r' | grep redis_version\|kividb_version | head -1)"; }
echo "== $LBL: $($B --version)"
for mode in none term-early kill9-early term-late kill9-late; do
  stop9; rm -rf $D; mkdir -p $D; start
  cli HSET h1 f1 v1 > /dev/null; cli HPEXPIRE h1 3000 FIELDS 1 f1 > /dev/null
  cli HSET h2 f1 v1 f2 v2 > /dev/null; cli HPEXPIRE h2 3000 FIELDS 2 f1 f2 > /dev/null
  cli HSET h3 f1 v1 f2 v2 > /dev/null; cli HPEXPIRE h3 3000 FIELDS 1 f1 > /dev/null
  t0=$(date +%s.%N); sleep 0.5
  case $mode in
    term-*)  p=$(srvpid); kill -TERM $p; while kill -0 $p 2>/dev/null; do sleep 0.05; done ;;
    kill9-*) stop9 ;;
  esac
  case $mode in *-late) sleep 4 ;; esac
  [ $mode != none ] && start
  while [ $(awk -v a=$t0 -v b=$(date +%s.%N) 'BEGIN{print (b-a<8)}') = 1 ]; do sleep 0.2; done
  r=""; for k in h1 h2 h3; do r="$r $k:exists=$(cli EXISTS $k),hlen=$(cli HLEN $k),type=$(cli TYPE $k)"; done
  echo "$mode:$r dbsize=$(cli DBSIZE)"
done
stop9
