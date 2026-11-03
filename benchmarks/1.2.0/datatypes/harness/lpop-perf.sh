#!/usr/bin/env bash
# lpop-perf.sh ENGINE: CPU profile of the server during the second LPOP run of
# the tail repro (the run that shows the 50-150 ms stalls): the hottest
# user-space functions, and the share of samples under glibc's malloc
# internals. Writes ~/out/lpop-perf/ENGINE.{folded.gz,svg,top.txt}.
set -u
ENGINE=$1 CLI=${CLI:-10.0.0.5} SRV=${SRV:-$(hostname -I | awk '{print $1}')} PORT=7001 FG=${FG:-$HOME/FlameGraph}
HERE=$(cd "$(dirname "$0")" && pwd)
source "$HERE/engines.sh"
OUT=~/out/lpop-perf; mkdir -p "$OUT"
MT="memtier_benchmark -s $SRV -p $PORT --hide-histogram --key-prefix=q: --key-minimum=1 --key-maximum=10000 \
  --distinct-client-seed --print-percentiles=50,99,99.9 -t 32 -c 4 --pipeline 16"
down "$ENGINE" "$PORT" 2> /dev/null
up "$ENGINE" "$PORT" || exit 1
PID=$(pids_of "$ENGINE" "$PORT" | tail -1)
ssh "$CLI" "$MT -n 312500 '--command=LPUSH __key__ __data__' --command-key-pattern=R -d 100 -R > /dev/null 2>&1"
ssh "$CLI" "$MT -n 93750 '--command=LPOP __key__' --command-key-pattern=R > /dev/null 2>&1"
ssh "$CLI" "$MT -n 93750 '--command=LPOP __key__' --command-key-pattern=R --json-out-file=/tmp/lpop2.json > /dev/null 2>&1" &
m=$!
sleep 0.3
sudo perf record -F 999 --call-graph fp -p "$PID" -o /tmp/lpop.data -- sleep 2 > /dev/null 2>&1
wait $m
ssh "$CLI" "python3 -c \"import json; t=json.load(open('/tmp/lpop2.json'))['ALL STATS']['Totals']; print('profiled run: %.0f ops/s p99.9 %.3f max %.3f ms' % (t['Ops/sec'], t['Percentile Latencies']['p99.90'], t['Max Latency']))\""
sudo perf script -i /tmp/lpop.data --no-inline 2> /dev/null | "$FG/stackcollapse-perf.pl" --kernel > /tmp/lpop.folded
gzip -c /tmp/lpop.folded > "$OUT/$ENGINE.folded.gz"
"$FG/flamegraph.pl" --minwidth 0.3 --width 1600 --title "$ENGINE, LPOP 100 B, pipeline 16, lists draining" < /tmp/lpop.folded > "$OUT/$ENGINE.svg"
awk '{n = $NF; t += n; if ($0 ~ /malloc_consolidate/) mc += n; if ($0 ~ /_int_free|cfree|__libc_free/) fr += n;
  if ($0 ~ /_int_malloc|__libc_malloc|malloc@/) ma += n}
  END {printf "samples %d: under malloc_consolidate %.1f%%, under free %.1f%%, under malloc %.1f%%\n", t, 100*mc/t, 100*fr/t, 100*ma/t}' /tmp/lpop.folded | tee "$OUT/$ENGINE.top.txt"
sudo perf report -i /tmp/lpop.data --no-children --sort symbol --stdio -g none 2> /dev/null | grep -v '^#' | grep '%' | head -15 | tee -a "$OUT/$ENGINE.top.txt"
down "$ENGINE" "$PORT"
