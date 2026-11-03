#!/usr/bin/env bash
# cg-measure.sh <tag>...: scripts/perf/measure.sh of release/1.2.0, run natively on a VM (as the rc9 fixes agent did:
# run.sh needs a Docker image that exists only on a laptop), with two changes for this retest:
#   - callgrind --separate-threads=yes: the count is the worker thread's (measure.sh already reads the largest
#     output file); whole-program counts move 1-2 % between runs of one binary (background threads);
#   - only GET-100, SET-100, MIX-100, INCR and DEL.
# Binaries: /target/perf-<tag>/release/kividb. Env as measure.sh: N, P, MODEL.
set -u
N=${N:-100000} P=${P:-16} MODEL=${MODEL:-tokio} PORT=6400
mkdir -p /out

# What the keys are loaded with before a workload: `-t set` by default, or
# the command in PRELOAD (redis-benchmark's own arguments, __rand_int__ and
# all). SERVER_EXTRA goes on the server's command line.
one() { # tag, label, redis-benchmark args...
  local tag=$1 label=$2; shift 2
  rm -rf /tmp/hp && mkdir -p /tmp/hp && cd /tmp/hp
  # shellcheck disable=SC2086
  valgrind --tool=callgrind --instr-atstart=no --separate-threads=yes --callgrind-out-file=/tmp/hp/out \
    "/target/perf-$tag/release/kividb" --port $PORT --bind 127.0.0.1 --io-model "$MODEL" \
    --threads 1 --loglevel warning ${SERVER_EXTRA:-} > /tmp/hp/log 2>&1 &
  local vg=$!
  for _ in $(seq 240); do redis-cli -p $PORT ping 2>/dev/null | grep -q PONG && break; sleep 0.5; done
  if ! redis-cli -p $PORT ping 2>/dev/null | grep -q PONG; then
    echo "$label $tag FAILED-TO-START"; tail -3 /tmp/hp/log; kill -9 $vg 2>/dev/null; return
  fi
  # Load and warm up: every key the measured run will touch exists.
  if [ -n "${PRELOAD:-}" ]; then
    # shellcheck disable=SC2086
    redis-benchmark -p $PORT -q -c 1 -P $P -r ${PRELOAD_R:-10000} -n ${PRELOAD_N:-20000} $PRELOAD > /dev/null 2>&1
  else
    redis-benchmark -p $PORT -q -c 1 -P $P -r 10000 -n 20000 -d "$DSIZE" -t set > /dev/null 2>&1
  fi
  redis-benchmark -p $PORT -q -c 1 -P $P -r 10000 -n 2000 "$@" > /dev/null 2>&1
  callgrind_control -i on > /dev/null 2>&1
  redis-benchmark -p $PORT -q -c 1 -P $P -r 10000 -n "$N" "$@" > /dev/null 2>&1
  callgrind_control -i off > /dev/null 2>&1
  callgrind_control -d > /dev/null 2>&1
  redis-cli -p $PORT shutdown nosave > /dev/null 2>&1
  sleep 1; kill -TERM $vg 2>/dev/null; wait $vg 2>/dev/null
  local f total
  f=$(ls -S /tmp/hp/out* 2>/dev/null | head -1)
  total=$(callgrind_annotate "$f" 2>/dev/null | grep -m1 "PROGRAM TOTALS" | tr -d "," | awk '{print $1}')
  echo "$label $tag $(( ${total:-0} / N ))"
  if [ "${PROFILE:-}" = "$tag" ]; then
    callgrind_annotate --inclusive=no "$f" 2>/dev/null | sed -n '1,70p' | cut -c1-200 > "/out/profile-$label.txt"
  fi
}

V100=$(printf 'x%.0s' $(seq 100))
LRU="--maxmemory 64gb --maxmemory-policy allkeys-lru"
LFU="--maxmemory 64gb --maxmemory-policy allkeys-lfu"
for tag in "$@"; do
  DSIZE=100;  one "$tag" GET-100  -d 100  -t get
  DSIZE=100;  one "$tag" SET-100  -d 100  -t set
  DSIZE=100;  one "$tag" MIX-100  -d 100  -t set,get
  PRELOAD="SET counter:__rand_int__ 0" DSIZE=100 one "$tag" INCR -t incr
  PRELOAD="-d 100 -t set" PRELOAD_R=200000 PRELOAD_N=400000 DSIZE=100 \
    one "$tag" DEL -r 200000 DEL key:__rand_int__
done | tee -a /out/raw.txt
echo done
