#!/usr/bin/env bash
# iothreads-sweep.sh: pick io-threads for Redis and Valkey on this 16-vCPU
# server: the hash group at 100 B (HGET, HGETALL, HSET; pipeline 1 and 16),
# one measured run per setting. The best setting is used for the matrix.
set -u
HERE=$(cd "$(dirname "$0")" && pwd)
for e in redis valkey; do
  for t in 4 6 8 12; do
    IO_THREADS=$t REPEATS=1 SIZES=100 GROUPS_=hash bash "$HERE/datatypes.sh" "$e" > /dev/null 2>&1
    rm -rf ~/out/sweep/"$e-io$t"; mkdir -p ~/out/sweep
    mv ~/out/datatypes/"$e" ~/out/sweep/"$e-io$t"
    ssh "${CLI:-10.0.0.5}" "rm -rf out/datatypes/$e"
    echo "$(date -u +%T) $e io-threads $t done"
  done
done
python3 "$HERE/summarize.py" ~/out/sweep ~/out/sweep-summary
cut -d, -f1-4,6,10-12,15,16 ~/out/sweep-summary/summary.csv | column -s, -t
echo SWEEP-DONE
