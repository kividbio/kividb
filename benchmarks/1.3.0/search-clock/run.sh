#!/usr/bin/env bash
# Instructions per search query, whole server, under callgrind, for two
# binaries (before and after a change), over hashes with and without field
# TTLs.
#
#   run.sh <label>=<binary> <label>=<binary> ...
#
# Env: DOCS (3000), Q (300 queries measured), KINDS (all five), MODEL (tokio).
# Needs valgrind and python3. Prints "kind ttl label instructions-per-query".
# Instruction counts do not move from run to run on the same binary, so two
# runs of one binary agree to the digit, and 1% is a real difference.
set -u
HERE=$(cd "$(dirname "$0")" && pwd)
DOCS=${DOCS:-3000} Q=${Q:-300} MODEL=${MODEL:-tokio} PORT=6410
KINDS=${KINDS:-"search-knn search-filter-knn search-tag aggregate-knn aggregate-filter-knn"}
for kind in $KINDS; do
  for ttl in 0 1; do
    for spec in "$@"; do
      label=${spec%%=*} bin=${spec#*=}
      rm -rf /tmp/sc && mkdir -p /tmp/sc && cd /tmp/sc || exit 1
      valgrind --tool=callgrind --instr-atstart=no --callgrind-out-file=/tmp/sc/out \
        "$bin" --port $PORT --bind 127.0.0.1 --io-model "$MODEL" --threads 1 \
        --loglevel warning > /tmp/sc/log 2>&1 &
      vg=$!
      for _ in $(seq 240); do redis-cli -p $PORT ping 2>/dev/null | grep -q PONG && break; sleep 0.5; done
      python3 -I "$HERE/bench.py" load $PORT "$DOCS" $ttl > /dev/null || { echo "$kind $ttl $label LOAD-FAILED"; kill -9 $vg; continue; }
      python3 -I "$HERE/bench.py" run $PORT 30 "$kind" || { echo "$kind $ttl $label RUN-FAILED"; kill -9 $vg; continue; }
      callgrind_control -i on > /dev/null 2>&1
      python3 -I "$HERE/bench.py" run $PORT "$Q" "$kind"
      callgrind_control -i off > /dev/null 2>&1
      callgrind_control -d > /dev/null 2>&1
      redis-cli -p $PORT shutdown nosave > /dev/null 2>&1
      sleep 1; kill -TERM $vg 2>/dev/null; wait $vg 2>/dev/null
      f=$(ls -S /tmp/sc/out* 2>/dev/null | head -1)
      total=$(callgrind_annotate "$f" 2>/dev/null | grep -m1 "PROGRAM TOTALS" | tr -d "," | awk '{print $1}')
      echo "$kind ttl=$ttl $label $(( ${total:-0} / Q ))"
    done
  done
done
