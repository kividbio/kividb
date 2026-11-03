#!/usr/bin/env bash
# Runs on the CLIENT after run_rc8.sh: 5 extra glove-25 loads (upload only) of
# rc8 and rc7, alternating which goes first, to settle the ingest gap.
set -u
export SERVER_IP=${SERVER_IP:?} DATASETS=glove-25-angular MODE=upload
for i in 1 2 3 4 5; do
  if [ $((i % 2)) = 1 ]; then E="kividb-rc8 kividb-rc7"; else E="kividb-rc7 kividb-rc8"; fi
  PASS=xg$i ENGINES="$E" bash ~/run_suite.sh
done
echo XG-DONE > ~/out/XG-DONE
