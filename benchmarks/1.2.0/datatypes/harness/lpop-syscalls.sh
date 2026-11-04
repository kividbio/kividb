#!/usr/bin/env bash
# lpop-syscalls.sh ENGINE: during the LPOP tail repro, count the memory system
# calls the server makes (madvise, munmap, brk, mmap) and how long its
# threads spend in them, to see whether the stalls come from memory going
# back to the kernel. 40M LPUSHes (100 B) on 10,000 lists, then LPOP runs.
set -u
ENGINE=$1 CLI=${CLI:-10.0.0.5} SRV=${SRV:-$(hostname -I | awk '{print $1}')} PORT=7001
HERE=$(cd "$(dirname "$0")" && pwd)
source "$HERE/engines.sh"
MT="memtier_benchmark -s $SRV -p $PORT --hide-histogram --key-prefix=q: --key-minimum=1 --key-maximum=10000 \
  --distinct-client-seed --print-percentiles=50,99,99.9 -t 32 -c 4 --pipeline 16"
down "$ENGINE" "$PORT" 2> /dev/null
up "$ENGINE" "$PORT" || exit 1
PID=$(pids_of "$ENGINE" "$PORT" | tail -1)
ssh "$CLI" "$MT -n 312500 '--command=LPUSH __key__ __data__' --command-key-pattern=R -d 100 -R > /dev/null 2>&1"
ssh "$CLI" "$MT -n 93750 '--command=LPOP __key__' --command-key-pattern=R > /tmp/lpop.txt 2>&1" &
m=$!
sleep 1
sudo perf trace -s -p "$PID" -e madvise,munmap,brk,mmap,mremap,futex -- sleep 3 2>&1 |
  awk '$1 ~ /^(madvise|munmap|brk|mmap|mremap|futex)$/ {c[$1] += $2; t[$1] += $4; if ($7 + 0 > m[$1]) m[$1] = $7 + 0}
    END {for (k in c) printf "%-8s calls %7d  total %9.1f ms  max %8.2f ms (3 s, all threads)\n", k, c[k], t[k], m[k]}'
sudo timeout 4 perf trace --duration 10 -p "$PID" -e madvise,munmap,brk,mmap,mremap 2>&1 | head -15
wait $m
ssh "$CLI" "grep -E '^Totals' /tmp/lpop.txt"
down "$ENGINE" "$PORT"
