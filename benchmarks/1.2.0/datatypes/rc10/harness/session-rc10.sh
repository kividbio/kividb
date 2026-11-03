#!/usr/bin/env bash
# on srv: the rc10 round of the datatypes suite (one engine on the server at a time).
#  - KiviDB rc10 (39bfacd2, v1.2.0-rc10) and rc8 (082dab3d), one round each of the full matrix
#    (both value sizes, pipeline 1 and 16, 3 measured runs per row), rc10 first
#  - no competitors, no LPOP tail repro, no memory pass (MEMPASS=0)
set -u
cd ~
log() { echo "$(date -u +%T) $*" | tee -a ~/master.log; }
run() { # engine run-name [env...]
  local e=$1 r=$2; shift 2
  log "START $r"
  env RUN="$r" MEMPASS=0 "$@" bash harness/datatypes.sh "$e" > ~/out/log-"$r".txt 2>&1
  grep -E "WARN|failed|ALL-DONE" ~/out/log-"$r".txt | tee -a ~/master.log
}
log "binaries: $(~/bin/kividb-rc10 --version) / $(~/bin/kividb-rc8 --version)"
run kividb-rc10 kividb-rc10
run kividb-rc8 kividb-rc8
log "matrix done"
python3 harness/summarize.py ~/out/datatypes ~/out/datatypes-summary | tee -a ~/master.log
log "MASTER-DONE"
