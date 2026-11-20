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
# WORKLOADS=sets and WORKLOADS=hashes: see their blocks below.
# WORKLOADS=tags runs what the hash-tag locks of a build with Lua cost a
# command (#889; `tag_locks`, on with lua-parallel-scripts): GET and SET of
# keys without a tag and of keys with one ({t}:<n>), a 10-key MGET, a
# one-key EVALSHA on a tagged key and on an untagged one (which runs alone,
# #880), and MULTI + 2 SET + EXEC (per
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
    # The same script on a key without a tag, which runs alone (#880).
    SETUP="SCRIPT LOAD $SCRIPT" DSIZE=100 one "$tag" EVALSHA-ALONE EVALSHA "$SHA" 1 "$K"
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
if [ "${WORKLOADS:-}" = sets ]; then
  # Set commands on one set of each form and size (#764 step 2): 14-byte
  # members `membr:<i>` (as suite 17's) in sets of 10 and 128 (a block),
  # 1,000, 100,000 and 3,000,000 (a table); integers 0..511 (an array).
  # The members asked for are uniform over those the set holds (a hit) or
  # not in it (a miss). Each set is loaded by SADDs of 1,000 members in
  # RESP through `redis-cli --pipe` (SIZES limits the sizes, e.g.
  # SIZES="str:10 int:512"). Rows counted per pair are a pair of commands
  # that leaves the set as big as it was: SREM then SADD of one member
  # (SREM's and SADD's writes, the set never leaving its form), and SPOP
  # then SADD of a new member. The large rows (SMEMBERS and SUNIONSTORE of
  # 100,000 members with 1,000 of them, SINTERSTORE of the same two (a result
  # of 1,000), SINTER of the 3,000,000 with a set
  # of 10 of its members, COPY of 1,000,000) are per command, NBIG (50) of
  # them, and 5 COPYs.
  gen() { # kind size seed n op -> RESP on stdout
    awk -v kind="$1" -v size="$2" -v seed="$3" -v n="$4" -v op="$5" '
      function m(i) { return kind == "int" ? sprintf("%d", i) : sprintf("membr:%08d", i) }
      function miss(i) { return kind == "int" ? sprintf("%d", size + i) : sprintf("absnt:%08d", i) }
      function out(a, b, c,   k) {
        k = (c == "") ? 2 : 3; if (b == "") k = 1
        printf "*%d\r\n$%d\r\n%s\r\n", k, length(a), a
        if (k >= 2) printf "$%d\r\n%s\r\n", length(b), b
        if (k == 3) printf "$%d\r\n%s\r\n", length(c), c
      }
      BEGIN {
        srand(seed)
        if (op == "load") {
          for (i = 0; i < size; i += 1000) {
            hi = (i + 1000 < size) ? i + 1000 : size
            printf "*%d\r\n$4\r\nSADD\r\n$1\r\ns\r\n", hi - i + 2
            for (j = i; j < hi; j++) { v = m(j); printf "$%d\r\n%s\r\n", length(v), v }
          }
          exit
        }
        fresh = 0
        for (q = 0; q < n; q++) {
          r = int(rand() * size)
          if (op == "SADD") out("SADD", "s", m(r))
          else if (op == "SISMEMBER-HIT") out("SISMEMBER", "s", m(r))
          else if (op == "SISMEMBER-MISS") out("SISMEMBER", "s", miss(r))
          else if (op == "SMISMEMBER") {
            a = m(r); b = m(int(rand() * size)); c = m(int(rand() * size)); d = miss(r)
            printf "*6\r\n$10\r\nSMISMEMBER\r\n$1\r\ns\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", length(a), a, length(b), b, length(c), c, length(d), d
          }
          else if (op == "SCARD") out("SCARD", "s")
          else if (op == "SRANDMEMBER") out("SRANDMEMBER", "s")
          else if (op == "SREM-SADD") { out("SREM", "s", m(r)); out("SADD", "s", m(r)) }
          else if (op == "SPOP-SADD") { out("SPOP", "s"); out("SADD", "s", kind == "int" ? sprintf("%d", size + 1000000 + fresh) : sprintf("fresh:%08d", fresh)); fresh++ }
          else if (op == "SMEMBERS") out("SMEMBERS", "s")
          else if (op == "SINTER") out("SINTER", "s", "small")
          else if (op == "SUNIONSTORE") printf "*4\r\n$11\r\nSUNIONSTORE\r\n$3\r\ndst\r\n$1\r\ns\r\n$5\r\nsmall\r\n"
          else if (op == "SINTERSTORE") printf "*4\r\n$11\r\nSINTERSTORE\r\n$3\r\ndst\r\n$1\r\ns\r\n$5\r\nsmall\r\n"
          else if (op == "COPY") { d = sprintf("copy:%d", q); printf "*3\r\n$4\r\nCOPY\r\n$1\r\ns\r\n$%d\r\n%s\r\n", length(d), d }
        }
      }'
  }
  small() { # kind k -> a set `small` of k members of `s`
    awk -v kind="$1" -v k="$2" 'BEGIN {
      printf "*%d\r\n$4\r\nSADD\r\n$5\r\nsmall\r\n", k + 2
      for (i = 0; i < k; i++) { v = (kind == "int") ? sprintf("%d", i * 7) : sprintf("membr:%08d", i * 7); printf "$%d\r\n%s\r\n", length(v), v }
    }'
  }
  for tag in "$@"; do
    for spec in ${SIZES:-str:10 str:128 int:512 str:1000 str:100000 str:3000000}; do
      kind=${spec%%:*} size=${spec##*:}
      case $spec in str:*) label="S$size" ;; *) label="I$size" ;; esac
      gen "$kind" "$size" 1 0 load > /tmp/load
      for op in SADD SISMEMBER-HIT SISMEMBER-MISS SMISMEMBER SCARD SRANDMEMBER SREM-SADD SPOP-SADD; do
        { cat /tmp/load; gen "$kind" "$size" 2 2000 "$op"; } > /tmp/set-warm
        gen "$kind" "$size" 3 "$N" "$op" > /tmp/set
        PIPEFILE=/tmp/set DSIZE=100 one "$tag" "$op-$label"
      done
      big=
      case $size in 100000) big="SMEMBERS SUNIONSTORE SINTERSTORE" ;; 3000000) big="SINTER" ;; esac
      for op in $big; do
        k=1000; [ "$op" = SINTER ] && k=10
        { cat /tmp/load; small "$kind" "$k"; gen "$kind" "$size" 2 2 "$op"; } > /tmp/set-warm
        gen "$kind" "$size" 3 "${NBIG:-50}" "$op" > /tmp/set
        N=${NBIG:-50} PIPEFILE=/tmp/set DSIZE=100 one "$tag" "$op-$label"
      done
    done
    if [ -z "${SIZES:-}" ] || [[ " $SIZES " == *" str:1000000 "* ]]; then
      gen str 1000000 1 0 load > /tmp/set-warm
      gen str 1000000 3 5 COPY > /tmp/set
      N=5 PIPEFILE=/tmp/set DSIZE=100 one "$tag" "COPY-S1000000"
    fi
  done | tee /out/raw.txt
  echo done
  exit 0
fi
if [ "${WORKLOADS:-}" = hashes ]; then
  # Hash commands on one hash of each form and size (#764 step 3): fields
  # `field:<i>` (14 bytes) with 32-byte values, as suite 19's, in hashes of
  # 10, 128 and 512 fields (a block) and 1,000, 100,000 and 3,000,000 (a
  # table), plus one integer field `n` for HINCRBY. The fields asked for are
  # uniform over those the hash holds (a hit) or not in it (a miss). Each
  # hash is loaded by HSETs of 1,000 fields in RESP through `redis-cli
  # --pipe` (SIZES limits the sizes, e.g. SIZES="10 1000"). HDEL-HSET is a
  # pair: HDEL of a field, then HSET of it again (a new field), which leaves
  # the hash as big as it was. HGETALL is per command, at up to 1,000
  # fields N of them, at 100,000 NBIG (50).
  hgen() { # size seed n op -> RESP on stdout
    awk -v size="$1" -v seed="$2" -v n="$3" -v op="$4" '
      function f(i) { return sprintf("field:%08d", i) }
      function v(i) { return sprintf("value:%026d", i) }
      function miss(i) { return sprintf("absnt:%08d", i) }
      function cmd(k, a, b, c, d,   x) {
        printf "*%d\r\n$%d\r\n%s\r\n", k, length(a), a
        printf "$1\r\nh\r\n"
        if (k >= 3) printf "$%d\r\n%s\r\n", length(b), b
        if (k >= 4) printf "$%d\r\n%s\r\n", length(c), c
        if (k >= 5) printf "$%d\r\n%s\r\n", length(d), d
      }
      BEGIN {
        srand(seed)
        if (op == "load") {
          for (i = 0; i < size; i += 1000) {
            hi = (i + 1000 < size) ? i + 1000 : size
            printf "*%d\r\n$4\r\nHSET\r\n$1\r\nh\r\n", 2 * (hi - i) + 2
            for (j = i; j < hi; j++) { a = f(j); b = v(j); printf "$%d\r\n%s\r\n$%d\r\n%s\r\n", length(a), a, length(b), b }
          }
          printf "*4\r\n$4\r\nHSET\r\n$1\r\nh\r\n$1\r\nn\r\n$1\r\n0\r\n"
          exit
        }
        for (q = 0; q < n; q++) {
          r = int(rand() * size)
          if (op == "HGET-HIT") cmd(3, "HGET", f(r))
          else if (op == "HGET-MISS") cmd(3, "HGET", miss(r))
          else if (op == "HSET") cmd(4, "HSET", f(r), v(r + 1))
          else if (op == "HDEL-HSET") { cmd(3, "HDEL", f(r)); cmd(4, "HSET", f(r), v(r)) }
          else if (op == "HINCRBY") cmd(4, "HINCRBY", "n", "1")
          else if (op == "HMGET") {
            a = f(r); b = f(int(rand() * size)); c = f(int(rand() * size)); d = miss(r)
            printf "*6\r\n$5\r\nHMGET\r\n$1\r\nh\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", length(a), a, length(b), b, length(c), c, length(d), d
          }
          else if (op == "HLEN") cmd(2, "HLEN")
          else if (op == "HEXISTS") cmd(3, "HEXISTS", f(r))
          else if (op == "HSTRLEN") cmd(3, "HSTRLEN", f(r))
          else if (op == "HGETALL") cmd(2, "HGETALL")
        }
      }'
  }
  for tag in "$@"; do
    for size in ${SIZES:-10 128 512 1000 100000 3000000}; do
      label="H$size"
      hgen "$size" 1 0 load > /tmp/load
      ops="HGET-HIT HGET-MISS HSET HDEL-HSET HINCRBY HMGET HLEN HEXISTS HSTRLEN"
      [ "$size" -le 1000 ] && ops="$ops HGETALL"
      for op in $ops; do
        { cat /tmp/load; hgen "$size" 2 2000 "$op"; } > /tmp/hash-warm
        hgen "$size" 3 "$N" "$op" > /tmp/hash
        PIPEFILE=/tmp/hash DSIZE=100 one "$tag" "$op-$label"
      done
      if [ "$size" = 100000 ]; then
        { cat /tmp/load; hgen "$size" 2 2 HGETALL; } > /tmp/hash-warm
        hgen "$size" 3 "${NBIG:-50}" HGETALL > /tmp/hash
        N=${NBIG:-50} PIPEFILE=/tmp/hash DSIZE=100 one "$tag" "HGETALL-$label"
      fi
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
