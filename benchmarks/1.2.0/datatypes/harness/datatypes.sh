#!/usr/bin/env bash
# datatypes.sh ENGINE: the data-type matrix against one engine. Runs on the
# server VM; memtier_benchmark runs on the client VM over ssh ($CLI).
#
# For each value size V (100 B, 2048 B) and each data-type group: FLUSHALL,
# load the group's keys, then for each command and pipeline depth (1, 16):
# one warm-up run (a quarter of the requests, not recorded) and $REPEATS
# measured runs of a fixed number of requests. Every engine gets the same
# key space, the same values and the same number of requests (kvrocks a
# quarter of it, NSCALE=4, because it is ~10x slower; rates are per second).
#
#   group  keys                         loaded with                         commands measured
#   hash   h:1..100000                  10 fields f0..f9 of V bytes         HGET f5, HGETALL (10 fields), HSET f0 (overwrite)
#   set    s:1..100000                  1 known member + 9 random, V bytes  SISMEMBER (known member: hit), SADD (new random member)
#   zset   z:1..100000                  10 random members, V bytes          ZRANGE 0 9 WITHSCORES, ZADD (new random member)
#   list   l:1..10000                   >= 100 elements of V bytes          LRANGE 0 99
#          q:1..10000                   empty                               LPUSH, then LPOP (3/4 as many pops as pushes)
#   incr   c:1..1000000                 (created by the warm-up)            INCR            (V = 100 only)
#
# Keys are drawn uniformly at random from the group's range; the h:, s:, ...
# prefixes are memtier --key-prefix (memtier's cluster mode routes on the
# generated key, so a prefix written into the command would be MOVED). SADD and ZADD
# add new random V-byte members (memtier --random-data mutates the value per
# request), so they insert, not overwrite. --distinct-client-seed gives every
# connection its own key and value sequence (without it all connections walk
# the same random keys in step and SADD/ZADD repeat each other's members).
#
# Output: ~/out/datatypes/ENGINE/*.json (memtier), server-cpu.csv,
# memory.csv (RSS after loading each group), versions.txt
set -u
ENGINE=$1
CLI=${CLI:-10.0.0.5} SRV=${SRV:-$(hostname -I | awk '{print $1}')} PORT=${PORT:-7001}
REPEATS=${REPEATS:-3} THREADS=${THREADS:-32} C1=${C1:-16} C16=${C16:-4}
NSCALE=${NSCALE:-1} SIZES=${SIZES:-"100 2048"} GROUPS_=${GROUPS_:-"hash set zset list incr"}
ONLY_P=${ONLY_P:-"1 16"}
HERE=$(cd "$(dirname "$0")" && pwd)
source "$HERE/engines.sh"
OUT=~/out/datatypes/$ENGINE ROUT=out/datatypes/$ENGINE
mkdir -p "$OUT"; ssh "$CLI" "mkdir -p $ROUT"
CLUSTER=""; [ "$ENGINE" = redis-cluster ] && CLUSTER="--cluster-mode"
[ "$ENGINE" = kvrocks ] && NSCALE=${NSCALE_KVROCKS:-4}
R() { redis-cli -p "$PORT" "$@"; }

cpu_ticks() { # summed utime+stime of every server process (and its threads), in clock ticks
  local t=0 pid
  for pid in $(pids_of "$ENGINE" "$PORT"); do
    t=$((t + $(awk '{print $14 + $15}' "/proc/$pid/stat" 2> /dev/null || echo 0)))
  done
  echo "$t"
}

# mt LABEL P TOTAL_REQUESTS memtier-args...: one memtier run on the client
mt() {
  local label=$1 p=$2 total=$3; shift 3
  local c=$C1; [ "$p" = 16 ] && c=$C16
  local n=$(( total / NSCALE / (THREADS * c) )); [ "$n" -lt 50 ] && n=50
  local t0 w0 t1 w1
  t0=$(cpu_ticks); w0=$(date +%s.%N)
  # shellcheck disable=SC2029
  ssh "$CLI" "ulimit -n 65535; memtier_benchmark -s $SRV -p $PORT $CLUSTER --hide-histogram --key-prefix= \
    -t $THREADS -c $c --pipeline $p -n $n --distinct-client-seed --print-percentiles=50,99,99.9 \
    --json-out-file=$ROUT/$label.json $(printf "'%s' " "$@") > $ROUT/$label.txt 2>&1"
  t1=$(cpu_ticks); w1=$(date +%s.%N)
  awk -v l="$label" -v t0="$t0" -v t1="$t1" -v w0="$w0" -v w1="$w1" -v hz="$(getconf CLK_TCK)" \
    'BEGIN {printf "%s,%.2f,%.3f\n", l, (t1 - t0) / hz, w1 - w0}' >> "$OUT/server-cpu.csv"
}

# bench NAME P TOTAL args...: warm-up + REPEATS measured runs
bench() {
  local name=$1 p=$2 total=$3; shift 3
  mt "warm-$name-p$p" "$p" $((total / 4)) "$@" > /dev/null
  for r in $(seq "$REPEATS"); do mt "$name-p$p-r$r" "$p" "$total" "$@"; done
  echo "$(date -u +%T) $ENGINE $name p$p done"
}

# load: every key of the range once, in key order (one connection: with more,
# memtier's allkeys has every client walk the whole range)
load() { # keymax args...
  local kmax=$1; shift
  # shellcheck disable=SC2029
  ssh "$CLI" "memtier_benchmark -s $SRV -p $PORT $CLUSTER --hide-histogram --key-prefix= -t 1 -c 1 --pipeline 32 \
    -n allkeys --key-minimum=1 --key-maximum=$((kmax + 1)) $(printf "'%s' " "$@") --command-key-pattern=P > $ROUT/load.txt 2>&1" \
    || { echo "load failed: $*"; ssh "$CLI" "head -3 $ROUT/load.txt"; }
}

flush() {
  if [ -n "$CLUSTER" ]; then
    for i in $(seq 0 $((SHARDS - 1))); do redis-cli -p $((PORT + i)) flushall > /dev/null; done
  else
    R flushall > /dev/null
  fi
  sleep 2
}

mem() { # label
  sleep 3
  local disk=0
  [ "$ENGINE" = kvrocks ] && disk=$(sudo du -sk "$KVROCKS_DIR/$PORT" | awk '{print $1}')
  echo "$1,$(rss_kb "$ENGINE" "$PORT"),$disk" >> "$OUT/memory.csv"
}

reps() { local s="" i; for i in $(seq "$1"); do s="$s $2"; done; echo "$s"; }

load_group() { # group V
  case $1 in
    hash) load 100000 --command="HSET __key__ $(for f in 0 1 2 3 4 5 6 7 8 9; do printf 'f%s __data__ ' $f; done)" --key-prefix=h: -d "$2" ;;
    set)
      load 100000 --command="SADD __key__ __data__" --key-prefix=s: -d "$2"
      load 100000 --command="SADD __key__$(reps 9 ' __data__')" --key-prefix=s: -d "$2" -R ;;
    zset) load 100000 --command="ZADD __key__ 1 __data__ 2 __data__ 3 __data__ 4 __data__ 5 __data__ 6 __data__ 7 __data__ 8 __data__ 9 __data__ 10 __data__" --key-prefix=z: -d "$2" -R ;;
    list) load 10000 --command="RPUSH __key__$(reps 100 ' __data__')" --key-prefix=l: -d "$2" ;;
  esac
}

down "$ENGINE" "$PORT" 2> /dev/null
echo "label,server_cpu_s,wall_s" > "$OUT/server-cpu.csv"
echo "label,rss_kb,disk_kb" > "$OUT/memory.csv"
# Memory pass: a fresh server per group, so nothing freed earlier sits in the RSS.
if [ "${MEMPASS:-1}" = 1 ]; then
  up "$ENGINE" "$PORT" || exit 1
  mem "empty"
  down "$ENGINE" "$PORT"
  for V in $SIZES; do
    for G in hash set zset; do
      up "$ENGINE" "$PORT" || exit 1
      load_group "$G" "$V"
      mem "$G-$V"
      down "$ENGINE" "$PORT"
    done
  done
fi
up "$ENGINE" "$PORT" || exit 1
version_of "$PORT" > "$OUT/versions.txt"
for V in $SIZES; do
  big=0; [ "$V" -gt 1000 ] && big=1
  for G in $GROUPS_; do
    case $G in
      hash)
        flush
        load_group hash "$V"
        [ "$(R hlen h:77777)" = 10 ] || [ -n "$CLUSTER" ] || echo "WARN hash load"
        for P in $ONLY_P; do
          if [ $big = 1 ]; then n=3000000; else n=$([ "$P" = 1 ] && echo 6000000 || echo 24000000); fi
          bench "hget-$V" "$P" $n --command="HGET __key__ f5" --key-prefix=h: --key-minimum=1 --key-maximum=100000 --command-key-pattern=R
        done
        for P in $ONLY_P; do
          if [ $big = 1 ]; then n=300000; else n=$([ "$P" = 1 ] && echo 2000000 || echo 4000000); fi
          bench "hgetall-$V" "$P" $n --command="HGETALL __key__" --key-prefix=h: --key-minimum=1 --key-maximum=100000 --command-key-pattern=R
        done
        for P in $ONLY_P; do
          if [ $big = 1 ]; then n=3000000; else n=$([ "$P" = 1 ] && echo 6000000 || echo 24000000); fi
          bench "hset-$V" "$P" $n --command="HSET __key__ f0 __data__" --key-prefix=h: -d "$V" -R --key-minimum=1 --key-maximum=100000 --command-key-pattern=R
        done ;;
      set)
        flush
        load_group set "$V"
        for P in $ONLY_P; do
          if [ $big = 1 ]; then n=3000000; else n=$([ "$P" = 1 ] && echo 6000000 || echo 24000000); fi
          bench "sismember-$V" "$P" $n --command="SISMEMBER __key__ __data__" --key-prefix=s: -d "$V" --key-minimum=1 --key-maximum=100000 --command-key-pattern=R
        done
        for P in $ONLY_P; do
          if [ $big = 1 ]; then n=1500000; else n=$([ "$P" = 1 ] && echo 6000000 || echo 16000000); fi
          bench "sadd-$V" "$P" $n --command="SADD __key__ __data__" --key-prefix=s: -d "$V" -R --key-minimum=1 --key-maximum=100000 --command-key-pattern=R
        done ;;
      zset)
        flush
        load_group zset "$V"
        for P in $ONLY_P; do
          if [ $big = 1 ]; then n=300000; else n=$([ "$P" = 1 ] && echo 3000000 || echo 6000000); fi
          bench "zrange-$V" "$P" $n --command="ZRANGE __key__ 0 9 WITHSCORES" --key-prefix=z: --key-minimum=1 --key-maximum=100000 --command-key-pattern=R
        done
        for P in $ONLY_P; do
          if [ $big = 1 ]; then n=1500000; else n=$([ "$P" = 1 ] && echo 6000000 || echo 16000000); fi
          bench "zadd-$V" "$P" $n --command="ZADD __key__ 1 __data__" --key-prefix=z: -d "$V" -R --key-minimum=1 --key-maximum=100000 --command-key-pattern=R
        done ;;
      list)
        flush
        load_group list "$V"
        for P in $ONLY_P; do
          if [ $big = 1 ]; then n=30000; else n=400000; fi
          bench "lrange100-$V" "$P" $n --command="LRANGE __key__ 0 99" --key-prefix=l: --key-minimum=1 --key-maximum=10000 --command-key-pattern=R
        done
        for P in $ONLY_P; do
          if [ $big = 1 ]; then n=1500000; else n=$([ "$P" = 1 ] && echo 6000000 || echo 16000000); fi
          bench "lpush-$V" "$P" $n --command="LPUSH __key__ __data__" --key-prefix=q: -d "$V" -R --key-minimum=1 --key-maximum=10000 --command-key-pattern=R
        done
        for P in $ONLY_P; do
          if [ $big = 1 ]; then n=1125000; else n=$([ "$P" = 1 ] && echo 4500000 || echo 12000000); fi
          bench "lpop-$V" "$P" $n --command="LPOP __key__" --key-prefix=q: --key-minimum=1 --key-maximum=10000 --command-key-pattern=R
        done ;;
      incr)
        [ "$V" = 100 ] || continue
        flush
        for P in $ONLY_P; do
          n=$([ "$P" = 1 ] && echo 6000000 || echo 24000000)
          bench "incr" "$P" $n --command="INCR __key__" --key-prefix=c: --key-minimum=1 --key-maximum=1000000 --command-key-pattern=R
        done ;;
    esac
  done
done
flush
down "$ENGINE" "$PORT"
scp -q -r "$CLI:$ROUT/*" "$OUT/"
echo "$(date -u +%T) $ENGINE ALL-DONE"
