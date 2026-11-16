#!/usr/bin/env bash
# Runs on the CLIENT: the whole 1.2.1 vector session, in order.
# KiviDB-only variant of run_all.sh (no competitors). Three full passes (searches + load), the two KiviDB builds
# alternating which goes first; pass 3 lingers 45 s after the tool's delete,
# for the memory-after-delete chart. Then the MEMORY PURGE check on both builds.
#   CAND  the build under test   (default kividb-121: ~/bins/121/kividb on the server)
#   BASE  the reference build    (default kividb-120: ~/bins/120/kividb, the 1.2.0 release binary)
# Usage: SERVER_IP=10.90.1.5 bash run_all.sh
set -u
SERVER_IP=${SERVER_IP:?}
CAND=${CAND:-kividb-121}
BASE=${BASE:-kividb-120}
SSH="ssh -o StrictHostKeyChecking=no ${VM_USER:-azureuser}@$SERVER_IP"
DS="h-and-m-2048-angular-filters dbpedia-openai-100K-1536-angular glove-25-angular dbpedia-openai-1M-1536-angular"
export SERVER_IP DATASETS="$DS"
$SSH 'echo 0 > ~/linger'
PASS=p1 MODE=full ENGINES="$CAND $BASE" bash ~/run_suite.sh
PASS=p2 MODE=full ENGINES="$BASE $CAND" bash ~/run_suite.sh
$SSH 'echo 45 > ~/linger'
PASS=p3 MODE=full ENGINES="$CAND $BASE" bash ~/run_suite.sh
$SSH 'echo 0 > ~/linger'
ENGINE=$CAND bash ~/purge_test.sh
ENGINE=$BASE bash ~/purge_test.sh
echo ALL-DONE > ~/out/ALL-DONE
