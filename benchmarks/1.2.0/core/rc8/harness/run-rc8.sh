#!/usr/bin/env bash
# rc8 round of the core suite, from load generator 1.
#   R  rc8 and rc7 alternating, two rounds (round 1: rc8 first; round 2: rc7 first),
#      the full suite (100 B / 1 KB / 2 KB, GET/SET p1/p16, depth sweep, MGET/MSET 10).
#   C  competitors, one round, 100 B GET p16, SET p16 and MGET/MSET 10 only.
#   PHASES="R C" (default); RROUNDS adds more R rounds by label, e.g. RROUNDS="3".
set -uo pipefail
H=$(cd "$(dirname "$0")" && pwd)
export SERVER=${SERVER:-10.40.0.10} CLIENT2=${CLIENT2:-10.40.0.12}
ulimit -n 1000000
OUT=${OUT:-$HOME/results}
for phase in ${PHASES:-R C}; do
  case $phase in
    R)
      for r in ${RROUNDS:-1 2}; do
        order="kividb-rc8 kividb-rc7"; [ $((r % 2)) = 0 ] && order="kividb-rc7 kividb-rc8"
        ENGINES="$order" ROUNDS="$r" OUT=$OUT bash "$H/run-suite.sh"
      done ;;
    C)
      ENGINES="garnet dragonfly redis@24" ROUNDS=1 SIZES=100 WL="get16 set16" SWEEP="" MULTI=yes \
        OUT=$OUT bash "$H/run-suite.sh" ;;
  esac
done
echo RC8-ALL-DONE
