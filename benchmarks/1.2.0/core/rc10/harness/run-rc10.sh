#!/usr/bin/env bash
# rc10 quick core check, from load generator 1: rc10 and rc8 alternating,
# two rounds (round 1: rc10 first; round 2: rc8 first), 100 B only:
# GET/SET at pipeline 1 and 16, then MGET/MSET of 10 keys. No depth sweep,
# no competitors.
set -uo pipefail
H=$(cd "$(dirname "$0")" && pwd)
export SERVER=${SERVER:-10.40.0.10} CLIENT2=${CLIENT2:-10.40.0.12}
ulimit -n 1000000
OUT=${OUT:-$HOME/results}
for r in ${RROUNDS:-1 2}; do
  order="kividb-rc10 kividb-rc8"; [ $((r % 2)) = 0 ] && order="kividb-rc8 kividb-rc10"
  ENGINES="$order" ROUNDS="$r" SIZES=100 WL="get1 get16 set1 set16" SWEEP="" MULTI=yes \
    OUT=$OUT bash "$H/run-suite.sh"
done
echo RC10-ALL-DONE
