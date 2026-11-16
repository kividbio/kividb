#!/usr/bin/env bash
# hset-perfstat.sh: on the server. Instructions and cycles per HSET for each build: load the 100 B
# hashes (h:1..100000 x 10 fields), then `perf stat` on the server process for one 24M-request
# HSET run at pipeline 16 (the matrix's run), alternating 121, 120, 121, 120.
# Prints build, ops/s, instructions/op, cycles/op, context switches, server CPU-seconds.
set -u
cd ~
CLI=${CLI:-10.0.0.5} SRV=$(hostname -I | awk '{print $1}') PORT=7001
source harness/engines.sh
for e in 121 120 121 120; do
  up kividb-$e $PORT > /dev/null || exit 1
  ssh "$CLI" "memtier_benchmark -s $SRV -p $PORT --hide-histogram --key-prefix=h: -t 1 -c 1 --pipeline 32 -n allkeys \
    --key-minimum=1 --key-maximum=100001 --command='HSET __key__ f0 __data__ f1 __data__ f2 __data__ f3 __data__ f4 __data__ f5 __data__ f6 __data__ f7 __data__ f8 __data__ f9 __data__' \
    -d 100 --command-key-pattern=P > /dev/null 2>&1"
  pid=$(pid_info $PORT)
  run() { ssh "$CLI" "ulimit -n 65535; memtier_benchmark -s $SRV -p $PORT --hide-histogram --key-prefix=h: -t 32 -c 4 --pipeline 16 -n $1 \
    --distinct-client-seed --command='HSET __key__ f0 __data__' -d 100 -R --key-minimum=1 --key-maximum=100000 --command-key-pattern=R 2>&1" | awk '/^Totals/ {print $2}'; }
  run 46875 > /dev/null # warm-up: a quarter
  sudo perf stat -x, -e instructions,cycles,context-switches,task-clock -p "$pid" -o /tmp/ps.txt -- sleep 600 &
  sp=$!
  sleep 0.5
  ops=$(run 187500)
  sudo pkill -x -f "sleep 600" # perf stat ends with its workload
  wait $sp 2> /dev/null
  ins=$(awk -F, '$3 ~ /^instructions/ {print $1}' /tmp/ps.txt)
  cyc=$(awk -F, '$3 ~ /^cycles/ {print $1}' /tmp/ps.txt)
  cs=$(awk -F, '$3 ~ /^context-switches/ {print $1}' /tmp/ps.txt)
  tc=$(awk -F, '$3 ~ /^task-clock/ {print $1}' /tmp/ps.txt)
  n=$((32 * 4 * 187500))
  awk -v e="$e" -v o="$ops" -v i="$ins" -v c="$cyc" -v s="$cs" -v t="$tc" -v n="$n" \
    'BEGIN {printf "kividb-%s ops/s=%.0f instructions/op=%.0f cycles/op=%.0f ctx-switches=%s cpu-s=%.1f\n", e, o, i / n, c / n, s, t / 1000}'
  down kividb-$e $PORT
done
