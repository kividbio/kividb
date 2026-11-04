#!/usr/bin/env bash
# Runs on the CLIENT after run_all.sh: 5 extra glove-25 loads (upload only) of
# the candidate and rc4, alternating which goes first, to settle the ingest gap.
set -u
export SERVER_IP=${SERVER_IP:?} DATASETS=glove-25-angular MODE=upload
for i in 1 2 3 4 5; do
  if [ $((i % 2)) = 1 ]; then E="kividb-cand kividb-rc4"; else E="kividb-rc4 kividb-cand"; fi
  PASS=xg$i ENGINES="$E" bash ~/run_suite.sh
done
echo XG-DONE > ~/out/XG-DONE
