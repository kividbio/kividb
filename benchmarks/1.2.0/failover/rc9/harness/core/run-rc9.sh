#!/usr/bin/env bash
# rc9 retest of the core suite, from load generator 1: rc9 and rc8 alternating, two rounds (round 1 rc9 first, round 2
# rc8 first), 100 B only: GET p1, GET p16, SET p16 and MSET of 10 keys (no depth sweep, no MGET).
set -uo pipefail
H=$(cd "$(dirname "$0")" && pwd)
export SERVER=${SERVER:-10.40.0.10} CLIENT2=${CLIENT2:-10.40.0.12}
ulimit -n 1000000
OUT=${OUT:-$HOME/results}
for r in 1 2; do
  order="kividb-rc9 kividb-rc8"; [ $((r % 2)) = 0 ] && order="kividb-rc8 kividb-rc9"
  ENGINES="$order" ROUNDS="$r" SIZES=100 WL="get1 get16 set16" SWEEP="" MULTI=yes MGET=no OUT=$OUT bash "$H/run-suite.sh"
done
~/venv/bin/python "$H/summarize.py" "$OUT" > "$OUT/summarize.out" 2>&1
~/venv/bin/python "$H/analyze-rc9.py" "$OUT" "$H/empty.csv" "$OUT/charts" > "$OUT/tables.md" 2> "$OUT/analyze.err"
echo RC9-CORE-DONE
