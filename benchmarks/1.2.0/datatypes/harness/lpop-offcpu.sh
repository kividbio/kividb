#!/usr/bin/env bash
# lpop-offcpu.sh ENGINE: scheduler trace (perf sched) of the server during the
# second LPOP run of the tail repro, then the longest off-CPU waits and the
# longest on-CPU stretches of its threads (perf sched timehist).
set -u
ENGINE=$1 CLI=${CLI:-10.0.0.5} SRV=${SRV:-$(hostname -I | awk '{print $1}')} PORT=7001
HERE=$(cd "$(dirname "$0")" && pwd)
source "$HERE/engines.sh"
MT="memtier_benchmark -s $SRV -p $PORT --hide-histogram --key-prefix=q: --key-minimum=1 --key-maximum=10000 \
  --distinct-client-seed --print-percentiles=50,99,99.9 -t 32 -c 4 --pipeline 16"
down "$ENGINE" "$PORT" 2> /dev/null
up "$ENGINE" "$PORT" || exit 1
PID=$(pids_of "$ENGINE" "$PORT" | tail -1)
ssh "$CLI" "$MT -n 312500 '--command=LPUSH __key__ __data__' --command-key-pattern=R -d 100 -R > /dev/null 2>&1"
ssh "$CLI" "$MT -n 93750 '--command=LPOP __key__' --command-key-pattern=R > /dev/null 2>&1"
ssh "$CLI" "$MT -n 93750 '--command=LPOP __key__' --command-key-pattern=R --json-out-file=/tmp/lpop2.json > /dev/null 2>&1" &
m=$!
sleep 0.5
sudo perf sched record -g -o /tmp/sched.data -- sleep 4 > /dev/null 2>&1
wait $m
ssh "$CLI" "python3 -c \"import json; t=json.load(open('/tmp/lpop2.json'))['ALL STATS']['Totals']; print('run: %.0f ops/s p99.9 %.3f max %.3f ms' % (t['Ops/sec'], t['Percentile Latencies']['p99.90'], t['Max Latency']))\""
echo "-- longest waits (off CPU) of the server's threads, ms"
sudo perf sched timehist -i /tmp/sched.data -p "$PID" 2> /dev/null | awk 'NR > 3 {print $4, $5, $6, $0}' | sort -k1 -rn | head -8 | cut -c1-160
echo "-- longest run times (on CPU without a switch), ms"
sudo perf sched timehist -i /tmp/sched.data -p "$PID" 2> /dev/null | awk 'NR > 3 {print $6, $0}' | sort -k1 -rn | head -5 | cut -c1-160
down "$ENGINE" "$PORT"
