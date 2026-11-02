#!/usr/bin/env bash
# Core string benchmark, driven from load generator 1. Starts each engine on
# the server, loads the dataset, and runs every scenario from BOTH load
# generators at once (memtier on cl1 here, memtier on cl2 over ssh), while
# sampling CPU and NIC counters on all three machines.
#
#   SERVER=10.40.0.10 CLIENT2=10.40.0.12 ENGINES="redis valkey" ROUNDS="1 2 3" \
#     bash run-suite.sh
#
# ENGINES   names understood by engine.sh; "redis@16" sets io-threads 16
# ROUNDS    round labels; each round runs every engine once, in ENGINES order
# SIZES     value sizes in bytes (default "100 1024 2048")
# WL        scenarios per size (default "get1 get16 set1 set16")
# MULTI     "yes": MSET/MGET of 10 keys after the 100-byte block (not on a cluster)
# SWEEP     GET pipeline depths run at 300 connections after the 100-byte block
#           (default "1 4 64"; "" to skip)
# DURATION  seconds per scenario (default 20); WARMUP (default 5)
# KEYS      key space (default 10,000,000), loaded fully before each size
# THREADS / CONNS_P16 / CONNS_P1   memtier per load generator (15 / 10 / 40):
#           2 x 15 x 10 = 300 connections pipelined, 2 x 15 x 40 = 1,200 not,
#           as in the README's run.
set -uo pipefail

SERVER=${SERVER:?server private ip}
CLIENT2=${CLIENT2:?second load generator private ip}
ENGINES=${ENGINES:?engines}
ROUNDS=${ROUNDS:-1}
SIZES=${SIZES:-"100 1024 2048"}
WL=${WL:-"get1 get16 set1 set16"}
MULTI=${MULTI:-yes}
SWEEP=${SWEEP-"1 4 64"}
DURATION=${DURATION:-20}
WARMUP=${WARMUP:-5}
KEYS=${KEYS:-10000000}
THREADS=${THREADS:-15}
CONNS_P16=${CONNS_P16:-10}
CONNS_P1=${CONNS_P1:-40}
OUT=${OUT:-$HOME/results}
H=$(cd "$(dirname "$0")" && pwd)
SSH=(ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ServerAliveInterval=15)
mkdir -p "$OUT"
log() { echo "$(date -u +%FT%TZ) $*" | tee -a "$OUT/suite.log"; }

half=$((KEYS / 2))

# Run one memtier invocation on both load generators at once, plus samplers.
#   both <dir> <name> <memtier args...>   (key range and output files added here)
both() {
  local dir=$1 name=$2; shift 2
  local skip=$((DURATION / 5)) win=$((DURATION * 3 / 5))
  local args=("$@")
  mkdir -p "$dir"
  "${SSH[@]}" "$SERVER" "bash ~/h/sample.sh $skip $win" > "$dir/$name.srv.cpu" 2>/dev/null &
  local ps=$!
  bash "$H/sample.sh" "$skip" "$win" > "$dir/$name.c1.cpu" 2>/dev/null &
  local p1s=$!
  timeout $((DURATION + 180)) memtier_benchmark "${args[@]}" \
    --json-out-file "$dir/$name.c1.json" --hdr-file-prefix "$dir/$name.c1" > "$dir/$name.c1.txt" 2>&1 &
  local p1=$!
  local q; q=$(printf '%q ' "${args[@]}")
  "${SSH[@]}" "$CLIENT2" "mkdir -p /tmp/r && rm -f /tmp/r/*; (bash ~/h/sample.sh $skip $win > /tmp/r/$name.c2.cpu 2>/dev/null &) ;
    timeout $((DURATION + 180)) memtier_benchmark $q --json-out-file /tmp/r/$name.c2.json --hdr-file-prefix /tmp/r/$name.c2 > /tmp/r/$name.c2.txt 2>&1; echo rc=\$? >> /tmp/r/$name.c2.txt; sleep 1" &
  local p2=$!
  wait $p1; local rc1=$?
  wait $p2
  wait $ps $p1s
  echo "rc=$rc1" >> "$dir/$name.c1.txt"
  scp -q -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR "$CLIENT2:/tmp/r/*" "$dir/" 2>/dev/null
  local t1 t2
  t1=$(grep -a "^Totals" "$dir/$name.c1.txt" | tail -1 | awk '{print $2}')
  t2=$(grep -a "^Totals" "$dir/$name.c2.txt" 2>/dev/null | tail -1 | awk '{print $2}')
  log "   $name: c1 ${t1:-FAIL} + c2 ${t2:-FAIL} ops/s; srv $(cat "$dir/$name.srv.cpu" 2>/dev/null) c1 $(cat "$dir/$name.c1.cpu" 2>/dev/null)"
}

for round in $ROUNDS; do
  for spec in $ENGINES; do
    engine=${spec%@*}; io=""
    [ "$spec" != "$engine" ] && io=${spec#*@}
    port=6379; cluster=(); mode=single
    [ "$engine" = redis-cluster ] && { port=7000; cluster=(--cluster-mode); mode=cluster; }
    dir0="$OUT/$spec/r$round"
    mkdir -p "$dir0"
    log "== round $round: $spec"
    if ! "${SSH[@]}" "$SERVER" "IO_THREADS=${io:-8} bash ~/h/engine.sh start $engine $port" > "$dir0/start.txt" 2>&1; then
      log "   start FAILED: $(tail -3 "$dir0/start.txt")"; continue
    fi
    cli() { redis-cli -h "$SERVER" -p "$port" "$@"; }
    for size in $SIZES; do
      dir="$dir0/d$size"; mkdir -p "$dir"
      if [ $mode = cluster ]; then redis-cli --cluster call "$SERVER:$port" flushall > /dev/null
      else cli flushall > /dev/null; fi
      common=(-s "$SERVER" -p "$port" "${cluster[@]}" --hide-histogram -d "$size")
      # Load: each generator writes its half of the key space once.
      t0=$(date +%s.%N)
      timeout 900 memtier_benchmark "${common[@]}" --ratio 1:0 --key-pattern P:P -n allkeys \
        --key-minimum=1 --key-maximum=$half -t "$THREADS" -c "$CONNS_P16" --pipeline 16 > "$dir/load.c1.txt" 2>&1 &
      l1=$!
      "${SSH[@]}" "$CLIENT2" "timeout 900 memtier_benchmark $(printf '%q ' "${common[@]}") --ratio 1:0 --key-pattern P:P -n allkeys \
        --key-minimum=$((half + 1)) --key-maximum=$KEYS -t $THREADS -c $CONNS_P16 --pipeline 16" > "$dir/load.c2.txt" 2>&1
      wait $l1
      t1=$(date +%s.%N)
      if [ $mode = cluster ]; then
        used=$(redis-cli --cluster call "$SERVER:$port" info memory | grep -a "used_memory:" | awk -F: '{s+=$NF} END {print s}')
        nkeys=$(redis-cli --cluster call "$SERVER:$port" dbsize | awk '{s+=$NF} END {print s}')
      else
        used=$(cli info memory | grep -a "^used_memory:" | tr -d '\r' | cut -d: -f2)
        nkeys=$(cli dbsize | tr -d '\r')
      fi
      echo "load_seconds=$(awk "BEGIN{printf \"%.1f\", $t1 - $t0}") used_memory=${used:-} keys=$nkeys" > "$dir/memory.txt"
      log "  d$size loaded: $(cat "$dir/memory.txt")"
      # Warm-up, discarded.
      run=(--key-minimum=1 --key-maximum=$KEYS --distinct-client-seed --key-pattern R:R
        --print-percentiles=50,99,99.9 -t "$THREADS")
      DURATION_SAVE=$DURATION; DURATION=$WARMUP
      both "$dir/warmup" warm "${common[@]}" "${run[@]}" --ratio 0:1 -c "$CONNS_P16" --pipeline 16 --test-time=$WARMUP > /dev/null
      DURATION=$DURATION_SAVE
      for w in $WL; do
        op=${w%%[0-9]*}; p=${w#$op}
        ratio=0:1; [ "$op" = set ] && ratio=1:0
        c=$CONNS_P16; [ "$p" = 1 ] && c=$CONNS_P1
        both "$dir" "${op}_p$p" "${common[@]}" "${run[@]}" --ratio $ratio -c "$c" --pipeline "$p" --test-time=$DURATION
      done
      # Pipeline-depth sweep, GET, 100 B, at a constant 300 connections
      # (get_p16 above is the depth-16 point).
      if [ "$size" = 100 ]; then
        for p in $SWEEP; do
          both "$dir" "sweep_get_p$p" "${common[@]}" "${run[@]}" --ratio 0:1 -c "$CONNS_P16" --pipeline "$p" --test-time=$DURATION
        done
      fi
      if [ "$MULTI" = yes ] && [ "$size" = 100 ] && [ $mode != cluster ]; then
        mset="MSET"; mget="MGET"
        for _ in $(seq 10); do mset+=" __key__ __data__"; mget+=" __key__"; done
        mk=(--key-minimum=1 --key-maximum=$KEYS --distinct-client-seed --print-percentiles=50,99,99.9
          -t "$THREADS" -c "$CONNS_P1" --pipeline 1 --test-time=$DURATION)
        # --command-key-pattern applies to the --command before it.
        both "$dir" mget10_p1 "${common[@]}" "${mk[@]}" "--command=$mget" --command-key-pattern=R
        both "$dir" mset10_p1 "${common[@]}" "${mk[@]}" "--command=$mset" --command-key-pattern=R
      fi
    done
    "${SSH[@]}" "$SERVER" "cp ~/logs/launch.log ~/launch-copy.log; tail -5 ~/logs/*.log 2>/dev/null | tail -40" > "$dir0/server-log-tail.txt" 2>&1
    "${SSH[@]}" "$SERVER" "bash ~/h/engine.sh stop" >> "$dir0/start.txt" 2>&1
  done
done
scp -q -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR "$SERVER:logs/launch.log" "$OUT/launch.log" 2>/dev/null
log "suite done: ENGINES=$ENGINES ROUNDS=$ROUNDS"
