#!/usr/bin/env bash
# Runs on the CLIENT. Does KiviDB give memory back after the data is gone?
# Load dbpedia-1M into a fresh KiviDB (no searches); the tool then drops the
# index and deletes every document. The server keeps sampling for 150 s; 60 s
# after the delete finished, MEMORY PURGE is sent.
# Usage: SERVER_IP=10.90.1.4 ENGINE=kividb-cand [PASS=purge] purge_test.sh
set -u
SERVER_IP=${SERVER_IP:?}
ENGINE=${ENGINE:-kividb-cand}
PASS=${PASS:-purge}
SSH="ssh -o StrictHostKeyChecking=no ${VM_USER:-azureuser}@$SERVER_IP"
DS=dbpedia-openai-1M-1536-angular
$SSH 'echo 150 > ~/linger'
SERVER_IP=$SERVER_IP PASS=$PASS MODE=upload DATASETS=$DS ENGINES=$ENGINE bash ~/run_suite.sh &
SUITE=$!
LOGF=~/out/$PASS/$ENGINE/$DS/tool.log
until grep -q "Experiment stage: Done" $LOGF 2>/dev/null; do sleep 1; done
sleep 60
echo "$(date +%s.%N | cut -c1-14) MEMORY PURGE -> $($SSH 'redis-cli -p 6380 MEMORY PURGE')" >> ~/out/$PASS/$ENGINE/$DS/purge.log
wait $SUITE
$SSH 'echo 0 > ~/linger'
echo PURGE-TEST-DONE >> ~/out/$PASS/$ENGINE/$DS/purge.log
