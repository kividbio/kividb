#!/usr/bin/env bash
# followup_a.sh: (1) unit/pause with the hanging test skipped, to see the rest of the unit;
# (2) unit/latency-monitor 5 times, on the 1.2.1 build and the 1.2.0 release build, to see whether
#     the expire-cycle latency test depends on how fast the machine frees a 1M-member set.
ulimit -n 65535
NEW=~/bin/kividb-lua+tls OLD=~/r120/kividb/kividb
cd ~/redis-tests
TAGS="--tags -external:skip --tags -needs:debug --tags -needs:repl --tags -needs:save --tags -needs:pfdebug --tags -large-memory --tags -cluster:skip --tags -keysizes"
run() { # run <bin> <unit> <log> [runtest args]
  local bin=$1 unit=$2 log=$3; shift 3
  local d; d=$(mktemp -d); (cd $d && exec $bin --port 6499 --loglevel error) > /dev/null 2>&1 &
  local pid=$!; for i in $(seq 50); do redis-cli -p 6499 PING >/dev/null 2>&1 && break; sleep 0.1; done
  timeout 600 ./runtest --host 127.0.0.1 --port 6499 --singledb --ignore-encoding --timeout 60 --single $unit $TAGS "$@" > $log 2>&1
  kill -9 $pid; wait $pid 2>/dev/null; rm -rf $d
  echo "$unit $(basename $bin) ok=$(grep -ac '^\[ok\]' $log) err=$(grep -ac '^\[err\]' $log) timeout=$(grep -ac '^\[TIMEOUT\]' $log)"
}
mkdir -p ~/followup
run $NEW unit/pause ~/followup/pause-skip-new.log --skiptest "Test RO scripts are not blocked by pause RO"
run $OLD unit/pause ~/followup/pause-old.log
for i in 1 2 3 4 5; do
  run $NEW unit/latency-monitor ~/followup/latmon-new-$i.log
  run $OLD unit/latency-monitor ~/followup/latmon-old-$i.log
done
grep -ah "\[err\]" ~/followup/*.log | sort | uniq -c
echo FOLLOWUP-DONE
