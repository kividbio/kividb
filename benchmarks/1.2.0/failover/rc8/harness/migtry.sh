#!/usr/bin/env bash
# migtry.sh <binary>: MIGRATE stopped by TRYAGAIN (rc8, commit 42a7282a). Based on rc8fixes/scripts/authcheck.sh.
# Two cluster nodes, each in its own directory. A key is written at the source while MIGRATE has it on the wire (the
# migrate-before-delete failpoint holds MIGRATE), for no auth, AUTH and AUTH2 at the destination, on both I/O models.
# Expect: TRYAGAIN saying the copies were deleted there; the source keeps the new value; the destination no longer
# holds the key; a second MIGRATE without REPLACE answers OK; the destination then holds the new value, the source not.
BIN=$1
W=/tmp/migtry
for model in tokio uring; do
for mode in none AUTH AUTH2; do
  { kill -9 ${PA:-} ${PB:-}; wait; } 2>/dev/null; sleep 0.3; rm -rf $W; mkdir -p $W/a $W/b
  (cd $W/a && KIVIDB_TEST_FAILPOINTS=migrate-before-delete exec $BIN --port 7600 --bind 127.0.0.1 --cluster-enabled yes --io-model $model > log 2>&1) &
  PA=$!
  extra=""; [ $mode != none ] && extra="--requirepass pw"
  (cd $W/b && exec $BIN --port 7601 --bind 127.0.0.1 --cluster-enabled yes --io-model $model $extra > log 2>&1) &
  PB=$!
  for _ in $(seq 50); do redis-cli -p 7600 ping >/dev/null 2>&1 && break; sleep 0.1; done
  for _ in $(seq 50); do redis-cli -p 7601 -a pw --no-auth-warning ping 2>/dev/null | grep -q PONG && break; sleep 0.1; done
  A="redis-cli -p 7600"; B="redis-cli -p 7601"; [ $mode != none ] && B="redis-cli -p 7601 -a pw --no-auth-warning"
  auth=""
  case $mode in
    AUTH) auth="AUTH pw";;
    AUTH2) $B ACL SETUSER mover on '>pw2' '~*' '+@all' >/dev/null; auth="AUTH2 mover pw2";;
  esac
  $A cluster addslotsrange 0 8191 >/dev/null; $B cluster addslotsrange 8192 16383 >/dev/null
  $A cluster meet 127.0.0.1 7601 >/dev/null
  for _ in $(seq 100); do [ "$($A cluster info | tr -d '\r' | grep cluster_state)" = cluster_state:ok ] && [ "$($B cluster info | tr -d '\r' | grep cluster_state)" = cluster_state:ok ] && break; sleep 0.2; done
  key=k:1; slot=$($A cluster keyslot $key); [ $slot -gt 8191 ] && { key=k:2; slot=$($A cluster keyslot $key); }
  $A set $key 1 >/dev/null
  $B cluster setslot $slot importing $($A cluster myid) >/dev/null
  $A cluster setslot $slot migrating $($B cluster myid) >/dev/null
  ( $A migrate 127.0.0.1 7601 "" 0 5000 $auth keys $key > $W/reply 2>&1 ) &
  mp=$!
  for _ in $(seq 100); do [ -e $W/a/failpoint.migrate-before-delete.1 ] && break; sleep 0.05; done
  incr=$($A incr $key)
  rm -f $W/a/failpoint.migrate-before-delete.1
  wait $mp
  reply=$(tr -d '\r' < $W/reply | cut -c1-160)
  src_val=$($A get $key)
  at_b=$( (echo ASKING; echo EXISTS $key) | $B | tail -1)
  ( $A migrate 127.0.0.1 7601 "" 0 5000 $auth keys $key > $W/again 2>&1 ) &
  mp=$!
  for _ in $(seq 100); do [ -e $W/a/failpoint.migrate-before-delete.2 ] && break; sleep 0.05; done
  rm -f $W/a/failpoint.migrate-before-delete.2
  wait $mp
  again=$(tr -d '\r' < $W/again)
  value=$( (echo ASKING; echo GET $key) | $B | tail -1)
  src_after=$($A exists $key)
  v=PASS
  case "$reply" in *TRYAGAIN*deleted*) ;; *) v=FAIL ;; esac
  # the source no longer holds the key: EXISTS answers 0, or -ASK to the destination (the slot is still MIGRATING)
  case "$src_after" in 0|ASK*) ;; *) v=FAIL ;; esac
  if [ "$incr" != 2 ] || [ "$src_val" != 2 ] || [ "$at_b" != 0 ] || [ "$again" != OK ] || [ "$value" != 2 ]; then v=FAIL; fi
  echo "$model $mode: INCR during MIGRATE=$incr | reply=[$reply] | source value=$src_val | EXISTS at destination after TRYAGAIN=$at_b | MIGRATE again (no REPLACE)=$again | destination value=$value | source EXISTS after=$src_after -> $v"
done
done
{ kill -9 $PA $PB; wait; } 2>/dev/null
