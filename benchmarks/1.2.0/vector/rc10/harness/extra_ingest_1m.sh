#!/usr/bin/env bash
# Runs on the CLIENT after run_rc10.sh: dbpedia-1M ingest read 1.1% slower for
# rc10 in the one pass, just beyond the rc8 round's load spread. Two more
# upload-only load pairs, alternating which build goes first (xg1: rc10 first,
# xg2: rc8 first), each into a freshly started server.
# Usage: SERVER_IP=<server private ip> bash extra_ingest_1m.sh
set -u
export SERVER_IP=${SERVER_IP:?}
DS=dbpedia-openai-1M-1536-angular
PASS=xg1 MODE=upload DATASETS=$DS ENGINES="kividb-rc10 kividb-rc8" bash ~/run_suite.sh
PASS=xg2 MODE=upload DATASETS=$DS ENGINES="kividb-rc8 kividb-rc10" bash ~/run_suite.sh
echo XG-DONE > ~/out/XG-DONE
