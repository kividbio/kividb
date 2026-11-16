#!/usr/bin/env bash
# benchall.sh (cl): core GET/SET rows (run-suite.sh, 4 alternating rounds), extra rows (bench_extra.sh, 4 rounds),
# then the datatypes hash + field-TTL groups driven from srv (session121.sh, 2 alternating rounds).
set -u
ulimit -n 1000000
export SERVER=10.40.0.10 CLIENT2=10.40.0.11
for r in 1 2 3 4; do
  order="kividb-121 kividb-120rel"; [ $((r % 2)) = 0 ] && order="kividb-120rel kividb-121"
  ENGINES="$order" ROUNDS="$r" SIZES=100 WL="get1 get16 set1 set16" SWEEP="" MULTI=no OUT=$HOME/results-core bash ~/h/run-suite.sh
done
NIC_MBPS=1560 python3 ~/h/summarize.py $HOME/results-core > /dev/null 2>&1; FLOOR=1 python3 ~/h/analyze-121.py $HOME/results-core > $HOME/results-core/tables.md 2>&1
echo CORE-DONE
OUT=$HOME/results-extra bash ~/h/bench_extra.sh
NIC_MBPS=1560 python3 ~/h/summarize.py $HOME/results-extra > /dev/null 2>&1
echo EXTRA-DONE
ssh 10.40.0.10 "bash ~/harness/session121.sh" > $HOME/dt-session.log 2>&1
echo BENCHALL-DONE
