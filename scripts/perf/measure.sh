#!/usr/bin/env bash
# Inside the container: instructions per command, whole server, pipelined.
#   measure.sh <tag>...
# For each tag and workload: start the server under callgrind with
# instrumentation off, warm up and load, switch instrumentation on for N
# pipelined commands, switch it off. Ir / N is the server's whole cost per
# command: read, parse, execute, reply, write.
# Env: N (100000), P (16), MODEL (tokio), PROFILE=<tag> prints that tag's
# hottest functions for each workload into /out/profile-<workload>.txt
# WORKLOADS=lists adds LPUSH, RPUSH and LPOP (full adds them too).
# WORKLOADS=full adds INCR and DEL, and GET, SET, INCR and DEL with a TTL on
# every key and under allkeys-lru, and GET, SET and INCR under allkeys-lfu
# (the memory redesign's matrix).
# WORKLOADS=access runs, for each tag, the commands that matter for
# `key-access-tracking` with it off and on (both explicit, so the result does
# not depend on the default): GET and SET with 100 B and 1 KB values,
# pipelined (-P 16) and not (-P 1); and pipelined INCR, HGET, LPUSH, LPOP and
# a 16-key MGET. Labels end in -no / -yes.
# WORKLOADS=tags runs what the hash-tag locks of a build with Lua cost a
# command (#889; `tag_locks`, on with lua-parallel-scripts): GET and SET of
# keys without a tag and of keys with one ({t}:<n>), a 10-key MGET, a
# one-key EVALSHA on a tagged key, and MULTI + 2 SET + EXEC (per
# transaction: N transactions of four commands). Run it on a build with
# FEATURES=lua,tls and on the default one (where EVALSHA is refused).
# CMP=plain preloads plaincmp.c's memcmp and bcmp into the server: counts
# that do not move with where the stack and the string literals happen to
# lie (see plaincmp.c). Compare two trees with the same CMP.
set -u
N=${N:-100000} P=${P:-16} MODEL=${MODEL:-tokio} PORT=6400
mkdir -p /out
CMP_PRELOAD=
if [ "${CMP:-}" = plain ]; then
  cc -O1 -fno-builtin -fno-tree-loop-distribute-patterns -shared -fPIC \
    -o /tmp/plaincmp.so "$(dirname "$0")/plaincmp.c" && CMP_PRELOAD=/tmp/plaincmp.so
fi

# What the keys are loaded with before a workload: `-t set` by default, or
# the command in PRELOAD (redis-benchmark's own arguments, __rand_int__ and
# all). SERVER_EXTRA goes on the server's command line.
one() { # tag, label, redis-benchmark args...
  local tag=$1 label=$2 P=${PIPE:-$P}; shift 2
  # ONLY=<regex> measures just the workloads whose label matches it.
  if [ -n "${ONLY:-}" ] && ! [[ $label =~ $ONLY ]]; then return; fi
  rm -rf /tmp/hp && mkdir -p /tmp/hp && cd /tmp/hp
  # SEED=<dir>: the server starts on that directory's data (a snapshot), and
  # so keeps the replication backlog for a replica that may come back.
  [ -n "${SEED:-}" ] && cp -r "$SEED"/. /tmp/hp/
  # shellcheck disable=SC2086
  LD_PRELOAD=$CMP_PRELOAD valgrind --tool=callgrind --instr-atstart=no --callgrind-out-file=/tmp/hp/out \
    "/target/perf-$tag/release/kividb" --port $PORT --bind 127.0.0.1 --io-model "$MODEL" \
    --threads 1 --loglevel warning ${SERVER_EXTRA:-} > /tmp/hp/log 2>&1 &
  local vg=$!
  for _ in $(seq 240); do redis-cli -p $PORT ping 2>/dev/null | grep -q PONG && break; sleep 0.5; done
  if ! redis-cli -p $PORT ping 2>/dev/null | grep -q PONG; then
    echo "$label $tag FAILED-TO-START"; tail -3 /tmp/hp/log; kill -9 $vg 2>/dev/null; return
  fi
  if [ -n "${SEED:-}" ] && ! redis-cli -p $PORT info replication | grep -q "repl_backlog_active:1"; then
    echo "$label $tag NO-BACKLOG"; kill -9 $vg 2>/dev/null; return
  fi
  # REPLICA=1: a replica attached (the same binary, not measured) before
  # anything is loaded.
  local rp=
  if [ -n "${REPLICA:-}" ]; then
    rm -rf /tmp/hp-r && mkdir -p /tmp/hp-r
    (cd /tmp/hp-r && exec "/target/perf-$tag/release/kividb" --port $((PORT + 1)) --bind 127.0.0.1 \
      --threads 1 --loglevel warning --replicaof 127.0.0.1 $PORT > /tmp/hp-r/log 2>&1) &
    rp=$!
    for _ in $(seq 240); do
      redis-cli -p $((PORT + 1)) info replication 2>/dev/null | grep -q "master_link_status:up" && break
      sleep 0.5
    done
    if ! redis-cli -p $PORT info replication | grep -q "connected_slaves:1"; then
      echo "$label $tag NO-REPLICA"; kill -9 $vg $rp 2>/dev/null; return
    fi
  fi
  # Load and warm up: every key the measured run will touch exists.
  if [ -n "${PRELOAD:-}" ]; then
    # shellcheck disable=SC2086
    redis-benchmark -p $PORT -q -c 1 -P $P -r ${PRELOAD_R:-10000} -n ${PRELOAD_N:-20000} $PRELOAD > /dev/null 2>&1
  else
    redis-benchmark -p $PORT -q -c 1 -P $P -r 10000 -n 20000 -d "$DSIZE" -t set > /dev/null 2>&1
  fi
  # SETUP: a command sent once before the warm-up (redis-cli's arguments).
  # shellcheck disable=SC2086
  [ -n "${SETUP:-}" ] && redis-cli -p $PORT $SETUP > /dev/null 2>&1
  if [ -n "${PIPEFILE:-}" ]; then
    # What redis-benchmark cannot send (a transaction): $PIPEFILE-warm,
    # then $PIPEFILE with N of them, in RESP, through `redis-cli --pipe`.
    redis-cli -p $PORT --pipe < "$PIPEFILE-warm" > /dev/null 2>&1
    callgrind_control -i on > /dev/null 2>&1
    redis-cli -p $PORT --pipe < "$PIPEFILE" > /dev/null 2>&1
  else
    redis-benchmark -p $PORT -q -c 1 -P $P -r 10000 -n 2000 "$@" > /dev/null 2>&1
    callgrind_control -i on > /dev/null 2>&1
    redis-benchmark -p $PORT -q -c 1 -P $P -r 10000 -n "$N" "$@" > /dev/null 2>&1
  fi
  callgrind_control -i off > /dev/null 2>&1
  callgrind_control -d > /dev/null 2>&1
  redis-cli -p $PORT shutdown nosave > /dev/null 2>&1
  sleep 1; kill -TERM $vg 2>/dev/null; wait $vg 2>/dev/null
  if [ -n "$rp" ]; then kill -TERM "$rp" 2>/dev/null; wait "$rp" 2>/dev/null; fi
  local f total
  f=$(ls -S /tmp/hp/out* 2>/dev/null | head -1)
  total=$(callgrind_annotate "$f" 2>/dev/null | grep -m1 "PROGRAM TOTALS" | tr -d "," | awk '{print $1}')
  echo "$label $tag $(( ${total:-0} / N ))"
  # EXEC=1: also what `execute_command` costs a command, everything under it
  # included: the command's own work, without the I/O loop around it, whose
  # count moves with how many commands each wake-up finds (tens of
  # instructions a command on io_uring between two runs of one binary).
  if [ -n "${EXEC:-}" ]; then
    local exec
    exec=$(callgrind_annotate --inclusive=yes "$f" 2>/dev/null | tr -d "," |
      awk '/handler::execute_command( |$)/ {print $1; exit}')
    echo "$label-EXEC $tag $(( ${exec:-0} / N ))"
  fi
  if [ "${PROFILE:-}" = "$tag" ]; then
    callgrind_annotate --inclusive=no "$f" 2>/dev/null | sed -n '1,70p' | cut -c1-200 > "/out/profile-$label.txt"
  fi
}

V100=$(printf 'x%.0s' $(seq 100))
LRU="--maxmemory 64gb --maxmemory-policy allkeys-lru"
LFU="--maxmemory 64gb --maxmemory-policy allkeys-lfu"
if [ "${WORKLOADS:-}" = stream ]; then
  # What feeding the replication stream costs a write (#894): SET-100 with
  # nothing reading the stream; with the backlog kept for a replica that may
  # come back (the server started from a snapshot, as after any restart);
  # with one replica attached; with the AOF on. GET-100 with nothing reading
  # it either. The hub's own work is in the whole-server rows, not in -EXEC.
  rm -rf /tmp/seed && mkdir -p /tmp/seed
  (cd /tmp/seed && exec "/target/perf-$1/release/kividb" --port $((PORT + 2)) --bind 127.0.0.1 \
    --threads 1 --loglevel warning > /tmp/seed/log 2>&1) &
  for _ in $(seq 60); do redis-cli -p $((PORT + 2)) ping 2>/dev/null | grep -q PONG && break; sleep 0.5; done
  redis-cli -p $((PORT + 2)) set seed 1 > /dev/null && redis-cli -p $((PORT + 2)) shutdown > /dev/null 2>&1
  wait
  rm -f /tmp/seed/log
  for tag in "$@"; do
    DSIZE=100; one "$tag" GET-100 -d 100 -t get
    DSIZE=100; one "$tag" SET-100 -d 100 -t set
    SEED=/tmp/seed DSIZE=100 one "$tag" SET-100-BACKLOG -d 100 -t set
    REPLICA=1 DSIZE=100 one "$tag" SET-100-REPLICA -d 100 -t set
    SERVER_EXTRA="--appendonly yes" DSIZE=100 one "$tag" SET-100-AOF -d 100 -t set
  done | tee /out/raw.txt
  echo done
  exit 0
fi
if [ "${WORKLOADS:-}" = tags ]; then
  # No space in the script: SETUP is split into redis-cli's arguments.
  SCRIPT="return(redis.call('GET',KEYS[1]))"
  SHA=$(printf '%s' "$SCRIPT" | sha1sum | cut -c1-40)
  # MULTI, SET, SET, EXEC on keys key:<n> with 100 B values, in RESP.
  txns() {
    awk -v n="$1" -v v="$V100" 'BEGIN {
      srand(1)
      for (i = 0; i < n; i++) {
        printf "*1\r\n$5\r\nMULTI\r\n"
        for (j = 0; j < 2; j++)
          printf "*3\r\n$3\r\nSET\r\n$16\r\nkey:%012d\r\n$100\r\n%s\r\n", int(rand() * 10000), v
        printf "*1\r\n$4\r\nEXEC\r\n"
      }
    }'
  }
  txns 2000 > /tmp/multi-warm
  txns "$N" > /tmp/multi
  K="key:__rand_int__"
  for tag in "$@"; do
    DSIZE=100; one "$tag" GET-100 -d 100 -t get
    DSIZE=100; one "$tag" SET-100 -d 100 -t set
    PRELOAD="SET {t}:__rand_int__ $V100" DSIZE=100 one "$tag" GET-TAG GET "{t}:__rand_int__"
    DSIZE=100 one "$tag" SET-TAG SET "{t}:__rand_int__" "$V100"
    DSIZE=100 one "$tag" MGET10 MGET $K $K $K $K $K $K $K $K $K $K
    PRELOAD="SET {t}:__rand_int__ $V100" SETUP="SCRIPT LOAD $SCRIPT" DSIZE=100 \
      one "$tag" EVALSHA-TAG EVALSHA "$SHA" 1 "{t}:__rand_int__"
    PIPEFILE=/tmp/multi DSIZE=100 one "$tag" MULTI-2SET
  done | tee /out/raw.txt
  echo done
  exit 0
fi
if [ "${WORKLOADS:-}" = zrand ]; then
  # ZRANDMEMBER on one sorted set of about 95,000 members (#862: 300,000
  # ZADDs of m:<n> with score n, n below 100,000), past any block: no count,
  # 1, 10, 1000, -10 WITHSCORES, -1000; ZSCORE of one member beside them.
  # A walk of the member table costs tens of thousands of instructions a
  # call here: run it with a small N (2000).
  Z="ZADD z __rand_int__ m:__rand_int__"
  for tag in "$@"; do
    export PRELOAD="$Z" PRELOAD_R=100000 PRELOAD_N=300000
    one "$tag" ZRANDMEMBER ZRANDMEMBER z
    one "$tag" ZRANDMEMBER-1 ZRANDMEMBER z 1
    one "$tag" ZRANDMEMBER-10 ZRANDMEMBER z 10
    one "$tag" ZRANDMEMBER-1000 ZRANDMEMBER z 1000
    one "$tag" ZRANDMEMBER-M10-WS ZRANDMEMBER z -10 WITHSCORES
    one "$tag" ZRANDMEMBER-M1000 ZRANDMEMBER z -1000
    one "$tag" ZSCORE ZSCORE z m:1
  done | tee /out/raw.txt
  echo done
  exit 0
fi
if [ "${WORKLOADS:-}" = access ]; then
  for tag in "$@"; do
    for mode in no yes; do
      export SERVER_EXTRA="--key-access-tracking $mode"
      for pipe in 16 1; do
        export PIPE=$pipe
        DSIZE=100;  one "$tag" "GET-100-p$pipe-$mode" -d 100  -t get
        DSIZE=100;  one "$tag" "SET-100-p$pipe-$mode" -d 100  -t set
        DSIZE=1024; one "$tag" "GET-1K-p$pipe-$mode"  -d 1024 -t get
        DSIZE=1024; one "$tag" "SET-1K-p$pipe-$mode"  -d 1024 -t set
      done
      export PIPE=16
      PRELOAD="SET counter:__rand_int__ 0" DSIZE=100 one "$tag" "INCR-$mode" -t incr
      PRELOAD="HSET hash:__rand_int__ f v" DSIZE=100 one "$tag" "HGET-$mode" HGET hash:__rand_int__ f
      DSIZE=100 one "$tag" "LPUSH-$mode" LPUSH list:__rand_int__ x
      PRELOAD="LPUSH list:__rand_int__ x" PRELOAD_N=400000 DSIZE=100 one "$tag" "LPOP-$mode" LPOP list:__rand_int__
      K="key:__rand_int__"
      DSIZE=100 one "$tag" "MGET16-$mode" MGET $K $K $K $K $K $K $K $K $K $K $K $K $K $K $K $K
    done
  done | tee /out/raw.txt
  echo done
  exit 0
fi
for tag in "$@"; do
  DSIZE=100;  one "$tag" GET-100  -d 100  -t get
  DSIZE=100;  one "$tag" SET-100  -d 100  -t set
  DSIZE=1024; one "$tag" GET-1K   -d 1024 -t get
  DSIZE=1024; one "$tag" SET-1K   -d 1024 -t set
  DSIZE=100;  one "$tag" MIX-100  -d 100  -t set,get
  if [ "${WORKLOADS:-}" = full ] || [ "${WORKLOADS:-}" = lists ]; then
    # One list (redis-benchmark's mylist), nobody blocked on it: the cost a
    # push or a pop pays for the blocking commands' lines when none waits.
    DSIZE=100; one "$tag" LPUSH -d 100 -t lpush
    DSIZE=100; one "$tag" RPUSH -d 100 -t rpush
    PRELOAD="-d 100 -t lpush" PRELOAD_N=200000 DSIZE=100 one "$tag" LPOP -d 100 -t lpop
  fi
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
    SERVER_EXTRA=$LFU DSIZE=100 one "$tag" GET-100-LFU -d 100 -t get
    SERVER_EXTRA=$LFU DSIZE=100 one "$tag" SET-100-LFU -d 100 -t set
    SERVER_EXTRA=$LFU PRELOAD="SET counter:__rand_int__ 0" DSIZE=100 one "$tag" INCR-LFU -t incr
  fi
done | tee /out/raw.txt
echo done
