#!/usr/bin/env bash
# Runs on the CLIENT: the whole rc7 vector session, in order.
# Three full passes (searches + load) over every engine, the candidate and
# rc4 alternating which goes first; pass 3 lingers 45 s after the tool's
# delete, for the memory-after-delete chart. Then the MEMORY PURGE check on
# the candidate and on rc4.
# The two KiviDB labels are kividb-<name>, where <name> is the second argument
# given to build_kividb.sh (the binary is ~/bins/<name>/kividb on the server):
#   CAND (default kividb-cand), BASE (default kividb-rc4).
# Usage: SERVER_IP=10.90.1.4 bash run_all.sh
set -u
SERVER_IP=${SERVER_IP:?}
CAND=${CAND:-kividb-cand}
BASE=${BASE:-kividb-rc4}
SSH="ssh -o StrictHostKeyChecking=no ${VM_USER:-azureuser}@$SERVER_IP"
DS="h-and-m-2048-angular-filters dbpedia-openai-100K-1536-angular glove-25-angular dbpedia-openai-1M-1536-angular"
export SERVER_IP DATASETS="$DS"
$SSH 'echo 0 > ~/linger'
PASS=p1 MODE=full ENGINES="$CAND $BASE redis redis-mt dragonfly" bash ~/run_suite.sh
PASS=p2 MODE=full ENGINES="$BASE $CAND dragonfly redis-mt redis" bash ~/run_suite.sh
$SSH 'echo 45 > ~/linger'
PASS=p3 MODE=full ENGINES="$CAND $BASE redis redis-mt dragonfly" bash ~/run_suite.sh
$SSH 'echo 0 > ~/linger'
ENGINE=$CAND bash ~/purge_test.sh
ENGINE=$BASE bash ~/purge_test.sh
echo ALL-DONE > ~/out/ALL-DONE
