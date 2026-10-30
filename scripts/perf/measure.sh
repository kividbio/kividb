#!/usr/bin/env bash
# Inside the container: instructions per command, whole server, pipelined.
#   measure.sh <tag>...
# For each tag and workload: start the server under callgrind with
# instrumentation off, warm up and load, switch instrumentation on for N
# pipelined commands, switch it off. Ir / N is the server's whole cost per
# command: read, parse, execute, reply, write.
# Env: N (100000), P (16), MODEL (tokio), PROFILE=<tag> prints that tag's
# hottest functions for each workload into /out/profile-<workload>.txt
# WORKLOADS=full adds INCR and DEL, and GET, SET, INCR and DEL with a TTL on
# every key and under allkeys-lru (the memory redesign's matrix).
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
  valgrind --tool=callgrind --instr-atstart=no --callgrind-out-file=/tmp/hp/out \
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
for tag in "$@"; do
  DSIZE=100;  one "$tag" GET-100  -d 100  -t get
  DSIZE=100;  one "$tag" SET-100  -d 100  -t set
  DSIZE=1024; one "$tag" GET-1K   -d 1024 -t get
  DSIZE=1024; one "$tag" SET-1K   -d 1024 -t set
  DSIZE=100;  one "$tag" MIX-100  -d 100  -t set,get
  if [ "${WORKLOADS:-}" = full ]; then
    # INCR's keys are counter:<n>; DEL runs on 200,000 loaded keys, so most
    # of the 60,000 it measures find their key (about four in five).
    PRELOAD="SET counter:__rand_int__ 0" DSIZE=100 one "$tag" INCR -t incr
    PRELOAD="-d 100 -t set" PRELOAD_R=200000 PRELOAD_N=400000 DSIZE=100 \
      one "$tag" DEL -r 200000 DEL key:__rand_int__
    PRELOAD="SET key:__rand_int__ $V100 EX 1000000" DSIZE=100 one "$tag" GET-100-TTL -t get
    PRELOAD="SET key:__rand_int__ $V100 EX 1000000" DSIZE=100 \
      one "$tag" SET-100-TTL SET key:__rand_int__ "$V100" EX 1000000
    PRELOAD="SET counter:__rand_int__ 0 EX 1000000" DSIZE=100 one "$tag" INCR-TTL -t incr
    PRELOAD="SET key:__rand_int__ $V100 EX 1000000" PRELOAD_R=200000 PRELOAD_N=400000 DSIZE=100 \
      one "$tag" DEL-TTL -r 200000 DEL key:__rand_int__
    SERVER_EXTRA=$LRU DSIZE=100 one "$tag" GET-100-LRU -d 100 -t get
    SERVER_EXTRA=$LRU DSIZE=100 one "$tag" SET-100-LRU -d 100 -t set
    SERVER_EXTRA=$LRU PRELOAD="SET counter:__rand_int__ 0" DSIZE=100 one "$tag" INCR-LRU -t incr
    SERVER_EXTRA=$LRU PRELOAD="-d 100 -t set" PRELOAD_R=200000 PRELOAD_N=400000 DSIZE=100 \
      one "$tag" DEL-LRU -r 200000 DEL key:__rand_int__
  fi
done | tee /out/raw.txt
echo done
