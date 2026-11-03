#!/usr/bin/env bash
# c3.sh <engine> <keys>: election safety. 3 shards (t1, t2, tc :7001) with a replica each (:7002 on the next VM).
# Shard A = t1:7001, its replica t2:7002. ~<keys> keys loaded into one slot of shard A. Then, under writes to A:
#   phase transfer: force a full sync of A's replica (kill -9, data deleted, nodes.conf kept, restart); kill -9 A's
#                   master while it sends the dataset; 25 s later, has the replica taken over?
#   phase load:     the same, killing the master while the replica loads.
#   failover:       replica caught up; kill -9 the master; time until the replica is master.
# The master is started again after each phase. Output in ~/c3-<engine>/ (or $OUT).
# rc9 retest: in the transfer and load phases ackcheck.py (cluster mode, 8 writers x 100 SET/s over every slot) runs
# for 45 s from the start of the phase, i.e. past the master's return; its acknowledged writes are read back once the
# master is back (its own verify, which retries until shard A answers), and again after the replica has synced
# (--verify-only, ack-<phase>-after/).
set -u
. $HOME/hosts.env
E=$1 KEYS=${2:-3000000} OUT=${OUT:-$HOME/c3-$1}
rm -rf $OUT; mkdir -p $OUT
A=$M B=$R C=$TCH
s() { ssh -o LogLevel=ERROR "$@"; }
rc() { timeout 3 redis-cli "$@" 2>/dev/null | tr -d '\r'; }
role() { rc -h $1 -p $2 ROLE | head -1; }
for h in $A $B $C; do s $h "bash cnode.sh wipeall; bash cnode.sh start $E 7001; bash cnode.sh start $E 7002" >> $OUT/start.txt; done
redis-cli --cluster create $A:7001 $B:7001 $C:7001 --cluster-yes > $OUT/create.log 2>&1
sleep 3
id() { rc -h $1 -p $2 CLUSTER MYID; }
redis-cli --cluster add-node $B:7002 $A:7001 --cluster-slave --cluster-master-id $(id $A 7001) >> $OUT/create.log 2>&1
redis-cli --cluster add-node $C:7002 $B:7001 --cluster-slave --cluster-master-id $(id $B 7001) >> $OUT/create.log 2>&1
redis-cli --cluster add-node $A:7002 $C:7001 --cluster-slave --cluster-master-id $(id $C 7001) >> $OUT/create.log 2>&1
for _ in $(seq 1 120); do [ "$(rc -h $B -p 7002 INFO replication | awk -F: '/master_link_status/{print $2}')" = up ] && break; sleep 0.5; done
# a hash tag in shard A's slots (0-5460)
TAG=""; for t in $(seq 0 500); do sl=$(rc -h $A -p 7001 CLUSTER KEYSLOT "{a$t}"); [ "$sl" -le 5460 ] && { TAG="{a$t}"; break; }; done
echo "tag $TAG slot $(rc -h $A -p 7001 CLUSTER KEYSLOT "$TAG")" | tee $OUT/tag.txt
V=$(printf 'x%.0s' $(seq 200))
redis-benchmark -h $A -p 7001 -q -c 50 -P 64 -n $KEYS -r 100000000 SET "$TAG:k:__rand_int__" $V > /dev/null 2>&1
echo "shard A keys $(rc -h $A -p 7001 DBSIZE), used_memory $(rc -h $A -p 7001 INFO memory | awk -F: '/^used_memory_human/{print $2}')" | tee -a $OUT/tag.txt
for _ in $(seq 1 240); do [ "$(rc -h $B -p 7002 INFO replication | awk -F: '/master_link_status/{print $2}')" = up ] && break; sleep 0.5; done
rc -h $A -p 7001 CLUSTER NODES > $OUT/nodes-start.txt
writes() { while :; do redis-benchmark -h $A -p 7001 -q -c 4 -n 100000 -r 1000000 SET "$TAG:w:__rand_int__" v > /dev/null 2>&1 || sleep 0.2; done; }
back_up() { # restart A's master, wait until the replica is streaming from it again
  s $A "bash cnode.sh restart 7001" >> $OUT/events.log
  for _ in $(seq 1 600); do
    [ "$(role $A 7001)" = master ] && [ "$(rc -h $B -p 7002 INFO replication | awk -F: '/master_link_status/{print $2}')" = up ] && break
    sleep 0.5
  done
  sleep 3
}
phase() { # name, condition to wait for before the kill
  local name=$1
  echo "$(date +%s.%N) phase-start" > $OUT/ack-$name.events
  $HOME/venv/bin/python $HOME/ackcheck.py --mode cluster --seeds $A:7001,$B:7001,$C:7001 --writers 8 --rate 100 \
    --duration 45 --out $OUT/ack-$name --events $OUT/ack-$name.events > $OUT/ack-$name.out 2>&1 &
  local AK=$!
  writes & local W=$!
  sleep 2
  s $B "bash cnode.sh resync 7002" >> $OUT/events.log
  local t0=$(date +%s.%N) seen=""
  for _ in $(seq 1 1200); do
    if [ $name = transfer ]; then
      rc -h $A -p 7001 INFO replication | grep -q -E '^slave[0-9]+:.*state=send_bulk' && { seen=1; break; }
    else
      [ "$(rc -h $B -p 7002 INFO replication | awk -F: '/master_sync_in_progress/{print $2}')" = 1 ] && { seen=1; break; }
    fi
    sleep 0.05
  done
  local tk=$(date +%s.%N)
  s $A "bash cnode.sh kill9 7001"
  echo "$(date +%s.%N) master-kill9" >> $OUT/ack-$name.events
  kill $W 2>/dev/null; pkill -f '[r]edis-benchmark' 2>/dev/null
  local sip=$(rc -h $B -p 7002 INFO replication | awk -F: '/master_sync_in_progress/{print $2}')
  echo "$name: condition seen=${seen:-no} $(awk -v a=$t0 -v b=$tk 'BEGIN{printf "%.2f", b-a}') s after the resync started; replica master_sync_in_progress at the kill: $sip" | tee -a $OUT/result.txt
  local promoted=""
  for i in $(seq 1 250); do [ "$(role $B 7002)" = master ] && { promoted=$(awk -v a=$tk -v b=$(date +%s.%N) 'BEGIN{printf "%.1f", b-a}'); break; }; sleep 0.1; done
  echo "$name: replica role 25 s after kill -9 of its master: $(role $B 7002)${promoted:+ (promoted after $promoted s)}; its keys: $(rc -h $B -p 7002 DBSIZE)" | tee -a $OUT/result.txt
  rc -h $C -p 7001 CLUSTER NODES > $OUT/nodes-after-$name.txt
  rc -h $C -p 7001 CLUSTER INFO | grep -E 'cluster_state|slots_fail|slots_ok' | tr '\n' ' ' | sed "s/^/$name: /" | tee -a $OUT/result.txt; echo | tee -a $OUT/result.txt
  s $B "grep -E 'Not standing for election|Won election|Promoted' /data/7002/server.log | tail -3" | sed "s/^/$name: replica log: /" | tee -a $OUT/result.txt
  if [ -z "$promoted" ]; then back_up; else
    # the old master comes back as a replica of the promoted one; then hand A back
    s $A "bash cnode.sh restart 7001" >> $OUT/events.log; sleep 15
  fi
  wait $AK
  echo "$name: ackcheck once the master was back: $(tail -1 $OUT/ack-$name.out)" | tee -a $OUT/result.txt
  rm -rf $OUT/ack-$name-after; mkdir -p $OUT/ack-$name-after; cp $OUT/ack-$name/acked.json $OUT/ack-$name-after/
  $HOME/venv/bin/python $HOME/ackcheck.py --mode cluster --seeds $A:7001,$B:7001,$C:7001 --duration 45 --verify-only \
    --out $OUT/ack-$name-after > $OUT/ack-$name-after.out 2>&1
  echo "$name: ackcheck again after the replica synced: $(tail -1 $OUT/ack-$name-after.out)" | tee -a $OUT/result.txt
  echo "$name: after: A $(role $A 7001) keys $(rc -h $A -p 7001 DBSIZE); B $(role $B 7002) keys $(rc -h $B -p 7002 DBSIZE)" | tee -a $OUT/result.txt
}
phase transfer
phase load
# a normal failover of a caught-up replica
if [ "$(role $B 7002)" = slave ]; then
  writes & W=$!
  sleep 5
  tk=$(date +%s.%N)
  s $A "bash cnode.sh kill9 7001"
  promoted=""
  for i in $(seq 1 300); do [ "$(role $B 7002)" = master ] && { promoted=$(awk -v a=$tk -v b=$(date +%s.%N) 'BEGIN{printf "%.2f", b-a}'); break; }; sleep 0.1; done
  kill $W 2>/dev/null; pkill -f '[r]edis-benchmark' 2>/dev/null
  echo "failover: caught-up replica promoted after ${promoted:-never (30 s)} s; keys A-before ? B now $(rc -h $B -p 7002 DBSIZE)" | tee -a $OUT/result.txt
  s $B "grep -E 'Not standing for election|Won election' /data/7002/server.log | tail -2" | sed 's/^/failover: replica log: /' | tee -a $OUT/result.txt
fi
for h in $A $B $C; do for p in 7001 7002; do s $h "cat /data/$p/server.log" > $OUT/server-$h-$p.log; done; done
for h in $A $B $C; do s $h "bash cnode.sh wipeall"; done
echo C3-DONE $E
