#!/usr/bin/env bash
# tailload.sh <cl dir>: keeps the soak's full load on past the 120 min of cl_soak.sh's own load, for as long as the
# event driver still runs (rc10: the GETKEYSINSLOT-bound reshards took far longer than in rc8 and pushed the last
# events past 120 min). Waits for <cl>/soak.end, then runs the same load as cl_soak.sh: soakgen 100k ops/s, the three
# shard probes, ackcheck.py (16 x 100 SET/s) and owcheck.py (8 x 200, same hot-slots file), until <cl>/events has
# "events-done". Output: <cl>/tail/ (soak.csv, probe-*.csv/raw, ack/, ow/). The checkers stop on a stop file
# (STOP_FILE, the rc10 fixes run's setup-cc.sh change) and then read everything back as usual; their keys are
# ackt:* and owt:* instead of ack:* and ow:*.
set -u
CL=$1 T=$1/tail
. $HOME/clhosts.env
PY=$HOME/venv/bin/python
SEEDS=$C1:7001,$C2:7001,$C3:7001
mkdir -p $T
for f in ackcheck.py owcheck.py; do
  sed 's/while time.monotonic() - t0 < \(args\|a\).duration:/while time.monotonic() - t0 < \1.duration and not os.path.exists(os.environ.get("STOP_FILE", "\/nonexistent")):/' $HOME/$f > $HOME/tail-$f
  grep -q '^import os' $HOME/tail-$f || sed -i 's/^import argparse/import argparse\nimport os/' $HOME/tail-$f
  # their own key names, so that the main checkers' read-back at 120 min doesn't meet the tail's writes
  sed -i 's/"ack:%d:%d"/"ackt:%d:%d"/g; s/"ow:{%s}/"owt:{%s}/g' $HOME/tail-$f
  grep -q STOP_FILE $HOME/tail-$f || { echo "no stop file in tail-$f"; exit 1; }
done
until [ -f $CL/soak.end ]; do sleep 1; done
grep -q events-done $CL/events && { echo "events already done, no tail load"; exit 0; }
rm -f $T/stop; export STOP_FILE=$T/stop
date +%s.%N > $T/start
$PY $HOME/tail-ackcheck.py --mode cluster --seeds $SEEDS --writers 16 --rate 100 --duration 7200 --timeout 2 --out $T/ack --settle 10 > $T/ackcheck.out 2>&1 &
ACK=$!
$PY $HOME/tail-owcheck.py --seeds $SEEDS --writers 8 --rate 200 --duration 7200 --timeout 2 --hot-file $CL/hot-slots --out $T/ow > $T/owcheck.out 2>&1 &
OW=$!
PR=""
for x in "a 2990-3010" "b 7990-8010" "c 12990-13010"; do
  set -- $x
  $HOME/shardprobe -addr $SEEDS -slots $2 -label $1 -conns 8 -rate 500 -duration 120m -stop-file $T/stop -raw $T/probe-$1.raw > $T/probe-$1.csv 2> $T/probe-$1.err &
  PR="$PR $!"
done
$HOME/soakgen-cl -cluster -addr $SEEDS -conns 128 -rate 100000 -duration 120m -interval 1s -err-log $T/soak-errs.log > $T/soak.csv 2> $T/soakgen.err &
SG=$!
until grep -q events-done $CL/events; do sleep 1; done
touch $T/stop
kill $SG
date +%s.%N > $T/end
wait $ACK $OW $PR
echo TAIL-DONE
