#!/usr/bin/env bash
# Profile one KiviDB build on the server under one core scenario, from load
# generator 1, with both load generators driving it (as run-suite.sh does).
#
#   SERVER=10.40.0.10 CLIENT2=10.40.0.12 bash profile.sh kividb-cand [pipeline] [conns-per-thread]
#
# Default: GET 100 B, pipeline 1, 2 x 15 x 10 = 300 connections (the
# "sweep_get_p1" row). The server records `perf record -g` of the KiviDB
# process for 15 s in the middle of a 40 s run, into ~/perf/<engine>-p<P>.data,
# and writes `perf report` text next to it.
set -uo pipefail
ENGINE=${1:?engine}
P=${2:-1}
CONNS=${3:-10}
SERVER=${SERVER:-10.40.0.10}
CLIENT2=${CLIENT2:-10.40.0.12}
KEYS=10000000
half=$((KEYS / 2))
SSH=(ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR)
OUT=$HOME/profile
mkdir -p "$OUT"

"${SSH[@]}" "$SERVER" "bash ~/h/engine.sh start $ENGINE 6379" > "$OUT/$ENGINE.start.txt" 2>&1 || exit 1
common=(-s "$SERVER" -p 6379 --hide-histogram -d 100)
memtier_benchmark "${common[@]}" --ratio 1:0 --key-pattern P:P -n allkeys --key-minimum=1 --key-maximum=$half \
  -t 15 -c 10 --pipeline 16 > /dev/null 2>&1 &
"${SSH[@]}" "$CLIENT2" "memtier_benchmark $(printf '%q ' "${common[@]}") --ratio 1:0 --key-pattern P:P -n allkeys \
  --key-minimum=$((half + 1)) --key-maximum=$KEYS -t 15 -c 10 --pipeline 16" > /dev/null 2>&1
wait
run=(--key-minimum=1 --key-maximum=$KEYS --distinct-client-seed --key-pattern R:R -t 15 -c "$CONNS"
  --ratio 0:1 --pipeline "$P" --test-time=40)
"${SSH[@]}" "$SERVER" "mkdir -p ~/perf; sleep 15; pid=\$(pgrep -x $ENGINE || pgrep -x kividb-1.1.3);
  sudo perf record -F 499 -g -p \$pid -o ~/perf/$ENGINE-p$P.data -- sleep 15 > /dev/null 2>&1;
  sudo chown \$USER ~/perf/$ENGINE-p$P.data" &
ps=$!
memtier_benchmark "${common[@]}" "${run[@]}" > "$OUT/$ENGINE-p$P.c1.txt" 2>&1 &
p1=$!
"${SSH[@]}" "$CLIENT2" "memtier_benchmark $(printf '%q ' "${common[@]}") $(printf '%q ' "${run[@]}")" > "$OUT/$ENGINE-p$P.c2.txt" 2>&1
wait $p1 $ps
"${SSH[@]}" "$SERVER" "cd ~/perf && perf report -i $ENGINE-p$P.data --no-children --sort symbol --stdio 2>/dev/null \
  | grep -v '^$' | grep -E '^ +[0-9]' | head -60 > $ENGINE-p$P.top.txt;
  perf report -i $ENGINE-p$P.data --no-children --sort dso --stdio 2>/dev/null | grep -E '^ +[0-9]' | head -15 > $ENGINE-p$P.dso.txt"
"${SSH[@]}" "$SERVER" "bash ~/h/engine.sh stop" > /dev/null 2>&1
t1=$(grep -a "^Totals" "$OUT/$ENGINE-p$P.c1.txt" | tail -1 | awk '{print $2}')
t2=$(grep -a "^Totals" "$OUT/$ENGINE-p$P.c2.txt" | tail -1 | awk '{print $2}')
echo "$ENGINE p$P: c1 $t1 + c2 $t2 ops/s"
