#!/usr/bin/env bash
# Runs on the CLIENT: the rc10 vector round.
# One pass of rc10 and rc8 (ef 128 points), alternating which goes first from
# dataset to dataset (rc10 first on h-and-m and glove-25, rc8 first on the two
# dbpedia sets). Each run starts a fresh server, loads the dataset once and
# runs every search point. No competitors, no purge check.
# Usage: SERVER_IP=<server private ip> bash run_rc10.sh
set -u
SERVER_IP=${SERVER_IP:?}
SSH="ssh -o StrictHostKeyChecking=no ${VM_USER:-azureuser}@$SERVER_IP"
export SERVER_IP
$SSH 'echo 0 > ~/linger'
PASS=p1 MODE=full DATASETS=h-and-m-2048-angular-filters     ENGINES="kividb-rc10 kividb-rc8" bash ~/run_suite.sh
PASS=p1 MODE=full DATASETS=dbpedia-openai-100K-1536-angular ENGINES="kividb-rc8 kividb-rc10" bash ~/run_suite.sh
PASS=p1 MODE=full DATASETS=glove-25-angular                 ENGINES="kividb-rc10 kividb-rc8" bash ~/run_suite.sh
PASS=p1 MODE=full DATASETS=dbpedia-openai-1M-1536-angular   ENGINES="kividb-rc8 kividb-rc10" bash ~/run_suite.sh
echo ALL-DONE > ~/out/ALL-DONE
