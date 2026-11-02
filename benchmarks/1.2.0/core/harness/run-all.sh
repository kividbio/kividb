#!/usr/bin/env bash
# The whole core run as it was done for 1.2.0-rc4, from load generator 1.
#
#   REDIS_IO=8 VALKEY_IO=8 bash run-all.sh
#
# 0. io-threads sweep for Redis and Valkey (100 B; GET p1, GET p16, SET p16),
#    one round each, to pick their thread count (see README).
# A. the competitors, three rounds, round-robin: every engine once per round.
# B. KiviDB 1.2.0-rc4 and 1.1.3 alternating, three rounds each, then one
#    more Redis round to check the machines had not drifted since A.
# Phase B waits for ~/bin/kividb-rc4 on the server (fetch-kividb.sh v1.2.0-rc4 rc4).
set -uo pipefail
H=$(cd "$(dirname "$0")" && pwd)
export SERVER=${SERVER:-10.40.0.10} CLIENT2=${CLIENT2:-10.40.0.12}
ulimit -n 1000000
PHASES=${PHASES:-"sweep A B"}

for phase in $PHASES; do
  case $phase in
    sweep)
      ENGINES="redis@4 redis@8 redis@16 redis@24 valkey@4 valkey@8 valkey@16 valkey@24" ROUNDS=s \
        SIZES=100 WL="get1 get16 set16" MULTI=no SWEEP="" OUT=$HOME/iosweep bash "$H/run-suite.sh" ;;
    A)
      ENGINES="redis@${REDIS_IO:?} redis-cluster valkey@${VALKEY_IO:?} dragonfly garnet" ROUNDS="1 2 3" \
        OUT=$HOME/results bash "$H/run-suite.sh" ;;
    B)
      until ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR "$SERVER" \
        test -x bin/kividb-rc4; do sleep 30; done
      ENGINES="kividb-rc4 kividb-113" ROUNDS="1 2 3" OUT=$HOME/results bash "$H/run-suite.sh"
      ENGINES="redis@${REDIS_IO:?}" ROUNDS="drift" OUT=$HOME/results-drift bash "$H/run-suite.sh" ;;
  esac
done
echo "run-all done"
