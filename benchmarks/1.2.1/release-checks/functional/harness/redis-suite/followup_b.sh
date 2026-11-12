#!/usr/bin/env bash
# followup_b.sh <io-model> <unit> <times>: run one unit <times> times on the 878746e4 build and on the
# 1.2.0 release binary (full variant), each on a fresh server forced to <io-model>, port 6499.
IO=$1 UNIT=$2 N=${3:-3}
ulimit -n 65535
NEW=~/bin/kividb-lua+tls OLD=~/r120/full/kividb/kividb
cd ~/redis-tests
TAGS="--tags -external:skip --tags -needs:debug --tags -needs:repl --tags -needs:save --tags -needs:pfdebug --tags -large-memory --tags -cluster:skip --tags -keysizes"
run() { # run <bin> <log>
  local bin=$1 log=$2
  local d; d=$(mktemp -d); (cd $d && exec $bin --port 6499 --io-model $IO --loglevel notice) > $log.server 2>&1 &
  local pid=$!; for i in $(seq 50); do redis-cli -p 6499 PING >/dev/null 2>&1 && break; sleep 0.1; done
  timeout 900 ./runtest --host 127.0.0.1 --port 6499 --singledb --ignore-encoding --timeout 300 --single $UNIT $TAGS > $log 2>&1
  kill -9 $pid; wait $pid 2>/dev/null; rm -rf $d
  echo "$UNIT $IO $(basename $(dirname $bin))/$(basename $bin) $(grep -a -m1 'I/O model' $log.server | sed 's/.*: //') ok=$(grep -ac '^\[ok\]' $log) err=$(grep -ac '^\[err\]' $log) timeout=$(grep -ac '^\[TIMEOUT\]' $log)"
}
u=$(echo $UNIT | tr / _)
mkdir -p ~/followup
for i in $(seq $N); do
  run $NEW ~/followup/$u-$IO-new-$i.log
  run $OLD ~/followup/$u-$IO-old-$i.log
done
grep -ah "\[err\]" ~/followup/$u-$IO-*.log | sort | uniq -c
echo FOLLOWUP-DONE
