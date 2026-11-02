#!/usr/bin/env bash
# profile.sh ENGINE: CPU profiles of one engine under GET and SET, 100-byte
# values, 1,000,000 keys, pipeline 1 and 16. Runs on the server VM; the load
# comes from memtier_benchmark on the client VM ($CLI), with the same
# connection counts as the data-type suite (32 threads x 16 connections at
# pipeline 1, 32 x 4 at pipeline 16).
#
# For each workload: 45 s of load; from second 12, for $SECS seconds:
#   - /proc utime and stime of the server (user vs kernel CPU, per request)
#   - perf stat: system calls, context switches
#   - perf record --call-graph fp at $FREQ Hz on the server's threads (frame pointers:
#     Redis builds with them; KiviDB is profiled as a build of the same tag with
#     -C force-frame-pointers=yes, because perf's DWARF unwinder returned empty
#     call chains for 15-50% of the release binary's samples)
# then the folded stacks (Brendan Gregg's stackcollapse-perf.pl) and a
# flamegraph SVG (flamegraph.pl --minwidth $MINW).
# Output: ~/out/profiling/ENGINE/{WL}-p{P}.{json,folded.gz,svg,stat.txt,cpu.csv}
set -u
ENGINE=$1
CLI=${CLI:-10.0.0.5} SRV=${SRV:-$(hostname -I | awk '{print $1}')} PORT=${PORT:-7001}
FREQ=${FREQ:-499} SECS=${SECS:-10} MINW=${MINW:-0.3} FG=${FG:-$HOME/FlameGraph}
WORKLOADS=${WORKLOADS:-"get set"} PIPES=${PIPES:-"1 16"}
HERE=$(cd "$(dirname "$0")" && pwd)
source "$HERE/engines.sh"
OUT=~/out/profiling/$ENGINE ROUT=out/profiling/$ENGINE
mkdir -p "$OUT"; ssh "$CLI" "mkdir -p $ROUT"
HZ=$(getconf CLK_TCK)

down "$ENGINE" "$PORT" 2> /dev/null
up "$ENGINE" "$PORT" || exit 1
version_of "$PORT" > "$OUT/versions.txt"
PID=$(pids_of "$ENGINE" "$PORT" | head -1)
# load every key once
ssh "$CLI" "memtier_benchmark -s $SRV -p $PORT --hide-histogram --key-prefix=key: -t 1 -c 1 --pipeline 64 \
  -n allkeys --key-minimum=1 --key-maximum=1000001 --key-pattern=P:P --ratio=1:0 -d 100 > /dev/null 2>&1"
redis-cli -p "$PORT" dbsize > "$OUT/dbsize.txt"
echo "workload,pipeline,secs,user_s,sys_s,ops_per_sec,user_us_per_op,sys_us_per_op,total_us_per_op,syscalls_per_op,ctxsw_per_op" > "$OUT/cpu.csv"
threads_snap() { # "tid comm ticks" for every thread of the server, sorted by tid
  local t
  for t in /proc/"$PID"/task/*; do
    awk -v tid="${t##*/}" '{c = $2; gsub(/[() ]/, "", c); print tid, c, $14 + $15}' "$t/stat" 2> /dev/null
  done | sort -k1,1
}

for WL in $WORKLOADS; do
  ratio=0:1; [ "$WL" = set ] && ratio=1:0
  for P in $PIPES; do
    c=16; [ "$P" = 16 ] && c=4
    tag=$WL-p$P
    # shellcheck disable=SC2029
    ssh "$CLI" "ulimit -n 65535; memtier_benchmark -s $SRV -p $PORT --hide-histogram --key-prefix=key: -t 32 -c $c \
      --pipeline $P --test-time 45 --ratio=$ratio --key-pattern=R:R --key-minimum=1 --key-maximum=1000000 -d 100 \
      --distinct-client-seed --print-percentiles=50,99,99.9 --json-out-file=$ROUT/$tag.json > $ROUT/$tag.txt 2>&1" &
    mpid=$!
    sleep 12
    # window 1: user/system CPU and counters (no sampling running)
    read -r u0 s0 < <(awk '{print $14, $15}' "/proc/$PID/stat")
    threads_snap > /tmp/th0
    sudo perf stat -e raw_syscalls:sys_enter,context-switches,task-clock -p "$PID" -o "$OUT/$tag.stat.txt" -- sleep "$SECS" 2> /dev/null
    read -r u1 s1 < <(awk '{print $14, $15}' "/proc/$PID/stat")
    threads_snap > /tmp/th1
    # per thread: CPU cores used during the window (is one thread the ceiling?)
    { echo "tid,thread,cores"; join /tmp/th0 /tmp/th1 | awk -v hz="$HZ" -v secs="$SECS" \
      '{printf "%s,%s,%.3f\n", $1, $2, ($5 - $3) / hz / secs}' | sort -t, -k3 -rn; } > "$OUT/$tag.threads.csv"
    # window 2: the samples for the flamegraph
    sudo perf record -F "$FREQ" --call-graph fp -p "$PID" -o "/tmp/$tag.data" -- sleep "$SECS" > /dev/null 2>&1
    wait $mpid
    scp -q "$CLI:$ROUT/$tag.json" "$CLI:$ROUT/$tag.txt" "$OUT/"
    ops=$(python3 -c "import json,sys; print(json.load(open(sys.argv[1]))['ALL STATS']['Totals']['Ops/sec'])" "$OUT/$tag.json")
    sc=$(awk '/raw_syscalls:sys_enter/ {gsub(",", "", $1); print $1}' "$OUT/$tag.stat.txt")
    cs=$(awk '/context-switches/ {gsub(",", "", $1); print $1}' "$OUT/$tag.stat.txt")
    awk -v w="$WL" -v p="$P" -v secs="$SECS" -v u0="$u0" -v u1="$u1" -v s0="$s0" -v s1="$s1" -v hz="$HZ" \
      -v ops="$ops" -v sc="${sc:-0}" -v cs="${cs:-0}" 'BEGIN {
        u = (u1 - u0) / hz; s = (s1 - s0) / hz; n = ops * secs;
        printf "%s,%s,%s,%.2f,%.2f,%.0f,%.3f,%.3f,%.3f,%.3f,%.4f\n", w, p, secs, u, s, ops,
          u * 1e6 / n, s * 1e6 / n, (u + s) * 1e6 / n, sc / n, cs / n }' >> "$OUT/cpu.csv"
    sudo perf script -i "/tmp/$tag.data" --no-inline 2> /dev/null | "$FG/stackcollapse-perf.pl" --kernel > "/tmp/$tag.folded"
    "$FG/flamegraph.pl" --minwidth "$MINW" --width 1600 --colors "${COLORS:-hot}" \
      --title "$ENGINE, ${WL^^} 100 B, pipeline $P ($(printf '%.2f' "$(echo "$ops" | awk '{print $1/1e6}')")M ops/s)" \
      --subtitle "perf record -F $FREQ --call-graph fp, $SECS s, all server threads; width = CPU time" \
      < "/tmp/$tag.folded" > "$OUT/$tag.svg"
    gzip -c "/tmp/$tag.folded" > "$OUT/$tag.folded.gz"
    sudo rm -f "/tmp/$tag.data"
    echo "$(date -u +%T) $ENGINE $tag done ($ops ops/s)"
  done
done
down "$ENGINE" "$PORT"
echo "$(date -u +%T) $ENGINE PROFILE-DONE"
