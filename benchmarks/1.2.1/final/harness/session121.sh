#!/usr/bin/env bash
# session121.sh (srv): the 1.2.1 datatypes suite's hash and field-TTL groups, 100 B, pipeline 1 and 16,
# two alternating rounds per build (round 1: 1.2.1 then 1.2.0; round 2: 1.2.0 then 1.2.1), 1 warm-up + 3 runs per row.
set -u
cd ~
log() { echo "$(date -u +%T) $*" | tee -a ~/dt-master.log; }
run() { local e=$1 r=$2; log "START $r"
  env CLI=10.40.0.12 RUN="$r" MEMPASS=0 SIZES=100 GROUPS_="hash hfe" bash harness/datatypes.sh "$e" > ~/out/log-"$r".txt 2>&1
  grep -E "WARN|failed|HFE" ~/out/log-"$r".txt | tee -a ~/dt-master.log; }
log "binaries: $(sha256sum ~/bin/kividb-121 | cut -c1-16) / $(sha256sum ~/bin/kividb-120 | cut -c1-16)"
run kividb-121 kividb-121
run kividb-120 kividb-120
run kividb-120 kividb-120-r2
run kividb-121 kividb-121-r2
python3 harness/summarize.py ~/out/datatypes ~/out/datatypes-summary | tail -3 | tee -a ~/dt-master.log
log "DT-DONE"
