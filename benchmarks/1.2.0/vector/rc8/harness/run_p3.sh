#!/usr/bin/env bash
# Runs on the CLIENT after run_rc8.sh and extra_glove_rc8.sh: a third full pass
# (rc8 first, as in p1) of the two datasets with points just outside the
# two-pass spread (h-and-m 1 client, dbpedia-1M 100 clients).
set -u
export SERVER_IP=${SERVER_IP:?}
PASS=p3 MODE=full DATASETS="h-and-m-2048-angular-filters dbpedia-openai-1M-1536-angular" \
  ENGINES="kividb-rc8 kividb-rc7" bash ~/run_suite.sh
echo P3-DONE > ~/out/P3-DONE
