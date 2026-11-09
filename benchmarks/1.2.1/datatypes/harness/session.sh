#!/usr/bin/env bash
# session.sh: on the server, two alternating rounds of the full matrix plus the field-TTL group:
#   round 1: 1.2.1 then 1.2.0;  round 2: 1.2.0 then 1.2.1
# Each round of an engine = harness/datatypes.sh (the 1.2.0 suite's script with the `hfe` group
# added), 1 warm-up + 3 measured runs per row, no memory pass.
set -u
cd ~
log() { echo "$(date -u +%T) $*" | tee -a ~/master.log; }
run() { # engine run-name
  local e=$1 r=$2
  log "START $r"
  env RUN="$r" MEMPASS=0 GROUPS_="hash hfe set zset list incr" bash harness/datatypes.sh "$e" > ~/out/log-"$r".txt 2>&1
  grep -E "WARN|failed|ALL-DONE|HFE" ~/out/log-"$r".txt | tee -a ~/master.log
}
log "binaries: $(~/bin/kividb-121 --version) / $(~/bin/kividb-120 --version)"
run kividb-121 kividb-121
run kividb-120 kividb-120
run kividb-120 kividb-120-r2
run kividb-121 kividb-121-r2
log "matrix done"
python3 harness/summarize.py ~/out/datatypes ~/out/datatypes-summary | tee -a ~/master.log
log "MASTER-DONE"
