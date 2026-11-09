#!/usr/bin/env bash
# 1.2.1 core check, from load generator 1: the builds alternate on the same VMs.
#   round 1: 1.2.1, 1.2.0 release binary, 1.2.0 release-way build
#   round 2: 1.2.0 release-way build, 1.2.0 release binary, 1.2.1
#   round 3+: odd = 1.2.1 then 1.2.0 release binary; even = the other way round
# Every round: 100 B, 1 KB and 2 KB, GET/SET at pipeline 1 (1,200 connections) and
# pipeline 16 (300 connections), 10M keys loaded per size, 5 s warm-up, 20 s per scenario,
# both load generators at once; MGET/MSET of 10 keys after the 100 B block. No depth sweep.
set -uo pipefail
H=$(cd "$(dirname "$0")" && pwd)
export SERVER=${SERVER:-10.40.0.10} CLIENT2=${CLIENT2:-10.40.0.12}
ulimit -n 1000000
OUT=${OUT:-$HOME/results}
for r in ${RROUNDS:-1 2}; do
  case $r in
    1) order="kividb-121 kividb-120rel kividb-120src" ;;
    2) order="kividb-120src kividb-120rel kividb-121" ;;
    *) order="kividb-121 kividb-120rel"; [ $((r % 2)) = 0 ] && order="kividb-120rel kividb-121" ;;
  esac
  ENGINES="$order" ROUNDS="$r" SIZES="${SIZES:-100 1024 2048}" WL="${WL:-get1 get16 set1 set16}" \
    SWEEP="" MULTI=${MULTI:-yes} OUT=$OUT bash "$H/run-suite.sh"
done
echo R121-ALL-DONE
