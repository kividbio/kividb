#!/usr/bin/env bash
# r303all.sh: the #303 runs, 2 reps: SIGTERM to A 0, 200 and 600 ms after B starts, and A never stopped.
O=${1:-$HOME/out/r303}; H=$(cd "$(dirname "$0")" && pwd)
for rep in 1 2; do
  for d in 0 200 600; do bash $H/r303load.sh stop $d $rep $O > /dev/null 2>&1; cat $O/stop-${d}ms-$rep/verdict.txt; done
  bash $H/r303load.sh never 0 $rep $O > /dev/null 2>&1; cat $O/never-0ms-$rep/verdict.txt
done
echo R303-ALL-DONE
