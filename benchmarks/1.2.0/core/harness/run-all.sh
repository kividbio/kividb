#!/usr/bin/env bash
# The whole core run as it was done for the 1.2.0 final numbers, from load
# generator 1.
#
#   PHASES="K A X drift" REDIS_IO=24 VALKEY_IO=24 bash run-all.sh
#
# sweep  io-threads sweep for Redis and Valkey (100 B; GET p1, GET p16,
#        SET p16), one round each, to pick their thread count (see README).
#        Not rerun for 1.2.0 final: same VM sizes, CPU and kernel as rc4.
# K      KiviDB: the 1.2.0 candidate, 1.2.0-rc4 and 1.1.3 in turn, three
#        rounds (candidate, rc4, 1.1.3, candidate, rc4, ...), so the
#        regression comparison shares one time window.
# A      the competitors, three rounds, round-robin: every engine once per round.
# X      one more candidate / rc4 round after A ($XROUNDS, default "4"), to
#        check the KiviDB numbers had not drifted over the session.
# drift  one more Redis round at the end, to check against phase A.
# K waits for ~/bin/kividb-cand and ~/bin/kividb-rc4 on the server (fetch-kividb.sh).
set -uo pipefail
H=$(cd "$(dirname "$0")" && pwd)
export SERVER=${SERVER:-10.40.0.10} CLIENT2=${CLIENT2:-10.40.0.12}
ulimit -n 1000000
PHASES=${PHASES:-"K A X drift"}
SSH=(ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR)

for phase in $PHASES; do
  case $phase in
    sweep)
      ENGINES="redis@4 redis@8 redis@16 redis@24 valkey@4 valkey@8 valkey@16 valkey@24" ROUNDS=s \
        SIZES=100 WL="get1 get16 set16" MULTI=no SWEEP="" OUT=$HOME/iosweep bash "$H/run-suite.sh" ;;
    K)
      until "${SSH[@]}" "$SERVER" 'test -x bin/kividb-cand && test -x bin/kividb-rc4'; do sleep 30; done
      ENGINES="kividb-cand kividb-rc4 kividb-113" ROUNDS="1 2 3" OUT=$HOME/results bash "$H/run-suite.sh" ;;
    A)
      ENGINES="redis@${REDIS_IO:?} redis-cluster valkey@${VALKEY_IO:?} dragonfly garnet" ROUNDS="1 2 3" \
        OUT=$HOME/results bash "$H/run-suite.sh" ;;
    X)
      ENGINES="kividb-cand kividb-rc4" ROUNDS="${XROUNDS:-4}" OUT=$HOME/results bash "$H/run-suite.sh" ;;
    drift)
      ENGINES="redis@${REDIS_IO:?}" ROUNDS="drift" OUT=$HOME/results-drift bash "$H/run-suite.sh" ;;
  esac
done
echo "run-all done"
