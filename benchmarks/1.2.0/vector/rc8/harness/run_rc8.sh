#!/usr/bin/env bash
# Runs on the CLIENT: the rc8 vector round.
# Two full passes of rc8 and rc7 (ef 128 points), alternating which goes first
# within every dataset; then Redis 8.10.2 and Dragonfly 2.0.2 once each on
# h-and-m and dbpedia-100K at 100 clients; then the MEMORY PURGE check on rc8.
# Usage: SERVER_IP=<server private ip> bash run_rc8.sh
set -u
SERVER_IP=${SERVER_IP:?}
SSH="ssh -o StrictHostKeyChecking=no ${VM_USER:-azureuser}@$SERVER_IP"
DS="h-and-m-2048-angular-filters dbpedia-openai-100K-1536-angular glove-25-angular dbpedia-openai-1M-1536-angular"
export SERVER_IP
$SSH 'echo 0 > ~/linger'
PASS=p1 MODE=full DATASETS="$DS" ENGINES="kividb-rc8 kividb-rc7" bash ~/run_suite.sh
PASS=p2 MODE=full DATASETS="$DS" ENGINES="kividb-rc7 kividb-rc8" bash ~/run_suite.sh
PASS=comp MODE=full DATASETS="h-and-m-2048-angular-filters dbpedia-openai-100K-1536-angular" \
  ENGINES="redis dragonfly" EXTRA_ARGS="--parallels 100" bash ~/run_suite.sh
ENGINE=kividb-rc8 bash ~/purge_test.sh
echo ALL-DONE > ~/out/ALL-DONE
