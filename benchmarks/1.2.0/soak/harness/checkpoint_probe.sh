#!/usr/bin/env bash
# checkpoint_probe.sh <master-ip> <replica-ip> <out-dir>     (env SETS: which of the set-ups below to run, in order)
# The soak's mix at 100k ops/s for 3 minutes, latency per 1 s, in three set-ups:
#   kividb-default   KiviDB, AOF on, default auto-checkpoint settings
#   kividb-min1gb    KiviDB, auto-aof-rewrite-min-size 1073741824 (fewer checkpoints)
#   kividb113-default  KiviDB 1.1.3 release binary, same settings (regression row)
#   kividb-noack     KiviDB, aof-ack-after-write no (acks once the record is queued, as 1.1 did)
#   redis            Redis 8.10.3, AOF on (everysec), default auto-aof-rewrite settings
# Each with one replica. The master's log gives the checkpoint / rewrite times.
set -u
M=$1 R=$2 OUT=$3
mkdir -p "$OUT"
s() { ssh -o StrictHostKeyChecking=no -o LogLevel=ERROR "$@"; }
run() { # run <name> <engine> <extra>
  s "$M" "ENGINE=$2 EXTRA='$3' bash server.sh start-master"
  s "$R" "ENGINE=$2 EXTRA='$3' bash server.sh start-replica $M"
  sleep 3
  "$HOME/soakgen" -addr "$M:6379" -conns 64 -prefill
  sleep 5
  date +%s.%N > "$OUT/$1.start"
  "$HOME/soakgen" -addr "$M:6379" -conns 128 -rate 100000 -duration 180s -interval 1s > "$OUT/$1.csv" 2>/dev/null
  if [ "$2" = redis ]; then :; fi
  if [ "$2" = redis ]; then
    s "$M" "grep -E 'Background append only file rewriting started|Background AOF rewrite finished' /mnt/kv/server.log" > "$OUT/$1.log"
  else
    s "$M" "grep -E 'auto-checkpoint|rebased onto dump.kdb' /mnt/kv/server.log" > "$OUT/$1.log"
  fi
  s "$M" "redis-cli INFO persistence | tr -d '\r' | grep -E 'aof_acks_before_write|aof_rewrites|aof_last_rewrite_time'" >> "$OUT/$1.info"
  echo "$1 done"
}
SETS=${SETS:-"kividb-default redis kividb-min1gb kividb113-default kividb-noack"}
for x in $SETS; do
  case $x in
    kividb-default)    run kividb-default kividb "" ;;
    kividb113-default) run kividb113-default kividb113 "" ;;
    kividb-noack)      run kividb-noack kividb "aof-ack-after-write no" ;;
    kividb-min1gb)     run kividb-min1gb kividb "auto-aof-rewrite-min-size 1073741824" ;;
    redis)             run redis redis "" ;;
  esac
done
s "$M" "bash server.sh stop"; s "$R" "bash server.sh stop"
echo PROBE-DONE
