#!/usr/bin/env bash
# c4.sh <engine> <rounds>: item 4, graceful handover. 3 shards (t1, t2, tc :7001) with a replica each (:7002 on the
# next VM). Shard A: t1:7001 and its replica t2:7002, 300k keys in one slot. Under writes to A (redis-benchmark SET,
# and SET ... PX 1500 so keys expire all the time), <rounds> times: CLUSTER FAILOVER on A's replica; when the roles
# have swapped and the demoted master's link is up, did it resync partially ("Partial resync: continuing") or in
# full? The two nodes swap roles each round. Output in ~/c4-<engine>/.
set -u
. $HOME/hosts.env
E=$1 N=${2:-10} OUT=${OUT:-$HOME/c4-$1}
rm -rf $OUT; mkdir -p $OUT
A=$M B=$R C=$TCH
s() { ssh -o LogLevel=ERROR "$@"; }
rc() { timeout 3 redis-cli "$@" 2>/dev/null | tr -d '\r'; }
role() { rc -h $1 -p $2 ROLE | head -1; }
link() { rc -h $1 -p $2 INFO replication | awk -F: '/master_link_status/{print $2}'; }
for h in $A $B $C; do s $h "bash cnode.sh wipeall; bash cnode.sh start $E 7001; bash cnode.sh start $E 7002" >> $OUT/start.txt; done
redis-cli --cluster create $A:7001 $B:7001 $C:7001 --cluster-yes > $OUT/create.log 2>&1
sleep 3
id() { rc -h $1 -p $2 CLUSTER MYID; }
redis-cli --cluster add-node $B:7002 $A:7001 --cluster-slave --cluster-master-id $(id $A 7001) >> $OUT/create.log 2>&1
redis-cli --cluster add-node $C:7002 $B:7001 --cluster-slave --cluster-master-id $(id $B 7001) >> $OUT/create.log 2>&1
redis-cli --cluster add-node $A:7002 $C:7001 --cluster-slave --cluster-master-id $(id $C 7001) >> $OUT/create.log 2>&1
for _ in $(seq 1 120); do [ "$(link $B 7002)" = up ] && break; sleep 0.5; done
TAG=""; for t in $(seq 0 500); do sl=$(rc -h $A -p 7001 CLUSTER KEYSLOT "{a$t}"); [ "$sl" -le 5460 ] && { TAG="{a$t}"; break; }; done
redis-benchmark -h $A -p 7001 -q -c 20 -P 32 -n 300000 -r 100000000 SET "$TAG:k:__rand_int__" vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv > /dev/null 2>&1
for _ in $(seq 1 120); do [ "$(link $B 7002)" = up ] && break; sleep 0.5; done
echo "tag $TAG; A keys $(rc -h $A -p 7001 DBSIZE)" | tee $OUT/result.txt
# writes follow whichever node of A is master (redis-cli -c follows MOVED)
writes() {
  while :; do
    for x in $A:7001 $B:7002; do
      h=${x%:*} p=${x#*:}
      [ "$(role $h $p)" = master ] || continue
      redis-benchmark -h $h -p $p -q -c 4 -n 20000 -r 1000000 SET "$TAG:w:__rand_int__" v > /dev/null 2>&1
      redis-benchmark -h $h -p $p -q -c 2 -n 5000 -r 1000000 SET "$TAG:x:__rand_int__" v PX 1500 > /dev/null 2>&1
    done
  done
}
writes & W=$!
sleep 10
M1=$A:7001 R1=$B:7002   # current master and replica of A
partial=0
for i in $(seq 1 $N); do
  mh=${M1%:*} mp=${M1#*:} rh=${R1%:*} rp=${R1#*:}
  s $mh "echo '=== round $i' >> /data/$mp/server.log"
  t0=$(date +%s.%N)
  r=$(rc -h $rh -p $rp CLUSTER FAILOVER)
  ok=""
  for _ in $(seq 1 600); do
    [ "$(role $rh $rp)" = master ] && [ "$(role $mh $mp)" = slave ] && [ "$(link $mh $mp)" = up ] && { ok=1; break; }
    sleep 0.1
  done
  t1=$(date +%s.%N)
  how=$(s $mh "awk '/=== round $i\$/{f=1} f' /data/$mp/server.log" | grep -o -E "Partial resync: continuing from offset [0-9]+|Full resync from|Full sync loaded|This node's dataset is offset [0-9]+" | tr '\n' ';')
  res=full; echo "$how" | grep -q "Partial resync: continuing" && { res=partial; partial=$((partial+1)); }
  [ -z "$ok" ] && res="not-done($r)"
  echo "round $i: CLUSTER FAILOVER on $R1 -> $r; swapped in $(awk -v a=$t0 -v b=$t1 'BEGIN{printf "%.1f", b-a}') s; demoted $M1 resync: $res [$how]" | tee -a $OUT/result.txt
  tmp=$M1; M1=$R1; R1=$tmp
  sleep 5
done
kill $W 2>/dev/null; pkill -f '[r]edis-benchmark' 2>/dev/null
echo "$E: $partial partial of $N" | tee -a $OUT/result.txt
for h in $A $B; do for p in 7001 7002; do s $h "cat /data/$p/server.log" > $OUT/server-$h-$p.log; done; done
for h in $A $B $C; do s $h "bash cnode.sh wipeall"; done
echo C4-DONE $E
