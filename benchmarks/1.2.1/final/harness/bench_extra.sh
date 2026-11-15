#!/usr/bin/env bash
# bench_extra.sh (load generator 1 = cl): rows the 1.2.1 suites do not have, run the core suite's way
# (both load generators at once, 20 s per scenario, CPU sampled over the middle 60%, builds alternating per round):
#   mixed_p16 / mixed_p1   SET:GET 1:4, 100 B, 1M keys loaded first, 300 / 1,200 connections
#   tsadd_p16 / tsadd_p1   TS.ADD ts:<k> * 1 over 100k series (made by the first adds)
#   knn_p1                 FT.SEARCH vidx "*=>[KNN 10 @vec $B]" on 50k FLOAT32 dim-32 vectors (HNSW, L2), 2 x 8 x 4 conns
#   ENGINES="kividb-121 kividb-120rel" ROUNDS="1 2 3 4" bash bench_extra.sh
set -uo pipefail
SERVER=${SERVER:-10.40.0.10} CLIENT2=${CLIENT2:-10.40.0.11}
ROUNDS=${ROUNDS:-"1 2 3 4"} DURATION=${DURATION:-20} THREADS=${THREADS:-15} CONNS_P16=${CONNS_P16:-10} CONNS_P1=${CONNS_P1:-40}
OUT=${OUT:-$HOME/results-extra}
SSH=(ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ServerAliveInterval=15)
mkdir -p "$OUT"
log() { echo "$(date -u +%FT%TZ) $*" | tee -a "$OUT/suite.log"; }
both() { # dir name memtier-args...   (as run-suite.sh's both())
  local dir=$1 name=$2; shift 2
  local skip=$((DURATION / 5)) win=$((DURATION * 3 / 5))
  local args=("$@")
  mkdir -p "$dir"
  "${SSH[@]}" "$SERVER" "bash ~/h/sample.sh $skip $win" > "$dir/$name.srv.cpu" 2>/dev/null &
  local ps=$!
  bash ~/h/sample.sh "$skip" "$win" > "$dir/$name.c1.cpu" 2>/dev/null &
  local p1s=$!
  timeout $((DURATION + 180)) memtier_benchmark "${args[@]}" \
    --json-out-file "$dir/$name.c1.json" > "$dir/$name.c1.txt" 2>&1 &
  local p1=$!
  local q; q=$(printf '%q ' "${args[@]}")
  "${SSH[@]}" "$CLIENT2" "mkdir -p /tmp/r && rm -f /tmp/r/*; (bash ~/h/sample.sh $skip $win > /tmp/r/$name.c2.cpu 2>/dev/null &) ;
    timeout $((DURATION + 180)) memtier_benchmark $q --json-out-file /tmp/r/$name.c2.json > /tmp/r/$name.c2.txt 2>&1; sleep 1" &
  local p2=$!
  wait $p1; wait $p2; wait $ps $p1s
  scp -q -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR "$CLIENT2:/tmp/r/*" "$dir/" 2>/dev/null
  local t1 t2
  t1=$(grep -a "^Totals" "$dir/$name.c1.txt" | tail -1 | awk '{print $2}')
  t2=$(grep -a "^Totals" "$dir/$name.c2.txt" 2>/dev/null | tail -1 | awk '{print $2}')
  log "   $name: c1 ${t1:-FAIL} + c2 ${t2:-FAIL} ops/s; srv $(cat "$dir/$name.srv.cpu" 2>/dev/null)"
}
for r in $ROUNDS; do
  order="kividb-121 kividb-120rel"; [ $((r % 2)) = 0 ] && order="kividb-120rel kividb-121"
  for e in $order; do
    d="$OUT/$e/r$r"; mkdir -p "$d"
    log "== round $r: $e"
    "${SSH[@]}" "$SERVER" "bash ~/h/engine.sh start $e 6379" > "$d/start.txt" 2>&1 || { log "start failed"; continue; }
    common=(-s "$SERVER" -p 6379 --hide-histogram --print-percentiles=50,99,99.9 --distinct-client-seed -t "$THREADS")
    # 1M keys of 100 B, each generator half
    memtier_benchmark -s $SERVER -p 6379 --hide-histogram -d 100 --ratio 1:0 --key-pattern P:P -n allkeys --key-minimum=1 --key-maximum=500000 -t 4 -c 4 --pipeline 16 > "$d/load.txt" 2>&1 &
    "${SSH[@]}" "$CLIENT2" "memtier_benchmark -s $SERVER -p 6379 --hide-histogram -d 100 --ratio 1:0 --key-pattern P:P -n allkeys --key-minimum=500001 --key-maximum=1000000 -t 4 -c 4 --pipeline 16" > /dev/null 2>&1
    wait
    log "  loaded $(redis-cli -h $SERVER dbsize) keys"
    mix=(-d 100 --ratio 1:4 --key-pattern R:R --key-minimum=1 --key-maximum=1000000)
    both "$d/d100" mixed_p16 "${common[@]}" "${mix[@]}" -c "$CONNS_P16" --pipeline 16 --test-time=$DURATION
    both "$d/d100" mixed_p1 "${common[@]}" "${mix[@]}" -c "$CONNS_P1" --pipeline 1 --test-time=$DURATION
    ts=(--key-prefix=ts: --key-minimum=1 --key-maximum=100000 "--command=TS.ADD __key__ * 1" --command-key-pattern=R)
    both "$d/d100" tsadd_p16 "${common[@]}" "${ts[@]}" -c "$CONNS_P16" --pipeline 16 --test-time=$DURATION
    both "$d/d100" tsadd_p1 "${common[@]}" "${ts[@]}" -c "$CONNS_P1" --pipeline 1 --test-time=$DURATION
    Q=$(~/venv/bin/python ~/knnload.py $SERVER 6379 50000 32)
    for _ in $(seq 120); do [ "$(redis-cli -h $SERVER FT.INFO vidx | grep -A1 '^indexing$' | tail -1)" = 0 ] && break; sleep 0.5; done
    log "  vectors: $(redis-cli -h $SERVER FT.INFO vidx | grep -A1 '^num_docs$' | tail -1)"
    both "$d/d100" knn_p1 -s "$SERVER" -p 6379 --hide-histogram --print-percentiles=50,99,99.9 -t 8 -c 4 --pipeline 1 --test-time=$DURATION \
      "--command=FT.SEARCH vidx \"*=>[KNN 10 @vec \$B]\" PARAMS 2 B $Q DIALECT 2 LIMIT 0 10 RETURN 0"
    redis-cli -h $SERVER INFO commandstats | tr -d '\r' | grep -E 'cmdstat_(get|set|ts.add|ft.search):' > "$d/commandstats.txt"
    redis-cli -h $SERVER INFO errorstats | tr -d '\r' > "$d/errorstats.txt"
    "${SSH[@]}" "$SERVER" "bash ~/h/engine.sh stop" >> "$d/start.txt" 2>&1
  done
done
log "EXTRA-DONE"
