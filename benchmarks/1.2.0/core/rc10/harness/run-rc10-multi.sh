#!/usr/bin/env bash
# Extra rounds 3 (rc10 first) and 4 (rc8 first). Meant as MGET/MSET only, but run-suite.sh
# reads WL="" as its default, so they ran the whole 100 B block; queued because the
# 2-round run left MSET 10 within 325 ops/s of failing the rule.
set -uo pipefail
H=$(cd "$(dirname "$0")" && pwd)
export SERVER=${SERVER:-10.40.0.10} CLIENT2=${CLIENT2:-10.40.0.12}
ulimit -n 1000000
OUT=${OUT:-$HOME/results-extra}
for r in 3 4; do
  order="kividb-rc10 kividb-rc8"; [ $((r % 2)) = 0 ] && order="kividb-rc8 kividb-rc10"
  ENGINES="$order" ROUNDS="$r" SIZES=100 WL="" SWEEP="" MULTI=yes OUT=$OUT bash "$H/run-suite.sh"
done
echo RC10-EXTRA-DONE
