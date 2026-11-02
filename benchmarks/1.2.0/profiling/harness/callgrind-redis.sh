#!/usr/bin/env bash
# callgrind-redis.sh: the measurement of the repo's scripts/perf/measure.sh
# (same redis-benchmark loads: one connection, pipeline 16, 10,000 keys,
# N commands measured with instrumentation switched on only for them),
# applied to redis-server 8.10.3 with its defaults (io-threads 1). Prints
# "LABEL redis-8.10.3 INSTRUCTIONS_PER_COMMAND".
set -u
N=${N:-60000} P=16 PORT=6410 RS=${RS:-$HOME/bin/redis-server}
one() { # label, redis-benchmark args...
  local label=$1; shift
  rm -rf /tmp/cgr && mkdir -p /tmp/cgr && cd /tmp/cgr
  # --set-proc-title no: Redis rewrites its environment for the process title, and
  # valgrind then cannot find the vgdb FIFO that callgrind_control talks through
  valgrind --tool=callgrind --instr-atstart=no --vgdb-prefix=/tmp/cgr/vgdb --callgrind-out-file=/tmp/cgr/out \
    "$RS" --port $PORT --bind 127.0.0.1 --save "" --appendonly no --set-proc-title no ${SERVER_EXTRA:-} > /tmp/cgr/log 2>&1 &
  local vg=$!
  for _ in $(seq 240); do redis-cli -p $PORT ping 2> /dev/null | grep -q PONG && break; sleep 0.5; done
  if [ -n "${PRELOAD:-}" ]; then
    # shellcheck disable=SC2086
    redis-benchmark -p $PORT -q -c 1 -P $P -r ${PRELOAD_R:-10000} -n ${PRELOAD_N:-20000} $PRELOAD > /dev/null 2>&1
  else
    redis-benchmark -p $PORT -q -c 1 -P $P -r 10000 -n 20000 -d "$DSIZE" -t set > /dev/null 2>&1
  fi
  redis-benchmark -p $PORT -q -c 1 -P $P -r 10000 -n 2000 "$@" > /dev/null 2>&1
  callgrind_control --vgdb-prefix=/tmp/cgr/vgdb -i on > /dev/null 2>&1
  redis-benchmark -p $PORT -q -c 1 -P $P -r 10000 -n "$N" "$@" > /dev/null 2>&1
  callgrind_control --vgdb-prefix=/tmp/cgr/vgdb -i off > /dev/null 2>&1
  callgrind_control --vgdb-prefix=/tmp/cgr/vgdb -d > /dev/null 2>&1
  redis-cli -p $PORT shutdown nosave > /dev/null 2>&1
  sleep 1; kill -TERM $vg 2> /dev/null; wait $vg 2> /dev/null
  local f total
  f=$(ls -S /tmp/cgr/out* 2> /dev/null | head -1)
  total=$(callgrind_annotate "$f" 2> /dev/null | grep -m1 "PROGRAM TOTALS" | tr -d "," | awk '{print $1}')
  echo "$label redis-8.10.3 $(( ${total:-0} / N ))"
  callgrind_annotate --inclusive=no "$f" 2> /dev/null | sed -n '1,60p' | cut -c1-200 > "$HOME/out/callgrind/redis-profile-$label.txt"
}
mkdir -p ~/out/callgrind
V100=$(printf 'x%.0s' $(seq 100))
DSIZE=100;  one GET-100  -d 100  -t get
DSIZE=100;  one SET-100  -d 100  -t set
DSIZE=1024; one GET-1K   -d 1024 -t get
DSIZE=1024; one SET-1K   -d 1024 -t set
DSIZE=100;  one MIX-100  -d 100  -t set,get
PRELOAD="SET counter:__rand_int__ 0" DSIZE=100 one INCR -t incr
PRELOAD="-d 100 -t set" PRELOAD_R=200000 PRELOAD_N=400000 DSIZE=100 one DEL -r 200000 DEL key:__rand_int__
PRELOAD="SET key:__rand_int__ $V100 EX 1000000" DSIZE=100 one GET-100-TTL -t get
