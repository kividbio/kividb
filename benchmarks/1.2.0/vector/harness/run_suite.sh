#!/usr/bin/env bash
# Runs on the CLIENT. For every dataset x engine: start the engine fresh on the
# server, start the server-side memory sampler, run vector-db-benchmark with
# vector-benchmark.yml's arguments, archive its results, stop the engine.
# Every tool output line is prefixed with the client's epoch time, and the
# client's CPU is recorded (mpstat) so a client-bound row is visible.
#
# Usage: SERVER_IP=10.90.1.4 PASS=p1 MODE=full|upload \
#        DATASETS="..." ENGINES="kividb-rc4 kividb-rc3 redis redis-mt dragonfly" \
#        EXTRA_ARGS="" run_suite.sh
#   MODE=upload adds --skip-search (ingest-only repeat).
set -u
SERVER_IP=${SERVER_IP:?}
PASS=${PASS:?}
MODE=${MODE:-full}
DATASETS=${DATASETS:-h-and-m-2048-angular-filters dbpedia-openai-100K-1536-angular glove-25-angular}
ENGINES=${ENGINES:-kividb-rc4 kividb-rc3 redis redis-mt dragonfly}
EXTRA_ARGS=${EXTRA_ARGS:-}
SSH="ssh -o StrictHostKeyChecking=no -o ConnectTimeout=30 -o ServerAliveInterval=30 ubuntu@$SERVER_IP"
OUT=~/out/$PASS
mkdir -p $OUT
LOG=$OUT/suite.log
ulimit -n 65535

tool_engine() { case $1 in kividb-*) echo kividb;; redis|redis-mt) echo redis;; dragonfly) echo dragonfly;; esac; }

for ds in $DATASETS; do
  for e in $ENGINES; do
    te=$(tool_engine $e)
    d=$OUT/$e/$ds
    mkdir -p $d
    echo "== $ds $e start $(date -u +%T)" >> $LOG
    if ! $SSH "bash ~/server_ctl.sh start $e" >> $LOG 2>&1; then
      echo "START FAILED $ds $e" >> $LOG; $SSH "bash ~/server_ctl.sh stop $e" >> $LOG 2>&1; continue
    fi
    $SSH "bash ~/server_ctl.sh sample $e /tmp/mem-$PASS-$e-$ds.csv" >> $LOG 2>&1
    sleep 3   # idle baseline samples before the load starts
    if [ "$ds" = glove-25-angular ]; then
      dir=~/vdb; args="--engines-file $te-engine.json --parallels 100"
    else
      dir=~/vdbx; args="--engines-file $te-engine.json --engines $te-m16-ef128"
    fi
    [ "$MODE" = upload ] && args="$args --skip-search"
    cd $dir
    rm -rf results
    nohup mpstat 1 > $d/client-mpstat.txt 2>&1 &
    MP=$!
    echo "$(date +%s.%N | cut -c1-14) RUN ./vector-db-benchmark --host $SERVER_IP $args --datasets $ds --skip-if-exists false $EXTRA_ARGS" > $d/tool.log
    KIVIDB_PORT=6380 REDIS_PORT=6381 DRAGONFLY_PORT=6382 ./vector-db-benchmark \
      --host "$SERVER_IP" $args --datasets "$ds" --skip-if-exists false $EXTRA_ARGS 2>&1 \
      | while IFS= read -r l; do echo "$(date +%s.%N | cut -c1-14) $l"; done >> $d/tool.log
    rc=${PIPESTATUS[0]}
    echo "$(date +%s.%N | cut -c1-14) EXIT $rc" >> $d/tool.log
    kill $MP 2>/dev/null
    cp -r results $d/results 2>/dev/null
    sleep 3   # trailing samples
    echo "exit=$rc $ds $e $(date -u +%T)" >> $LOG
    $SSH "bash ~/server_ctl.sh stop $e" >> $LOG 2>&1
    scp -q -o StrictHostKeyChecking=no ubuntu@$SERVER_IP:/tmp/mem-$PASS-$e-$ds.csv $d/mem.csv
  done
done
echo "SUITE DONE $PASS $(date -u +%T)" >> $LOG
