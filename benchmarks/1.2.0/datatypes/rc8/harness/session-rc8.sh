#!/usr/bin/env bash
# on srv: the rc8 round of the datatypes suite, in order (one engine on the server at a time).
#  - KiviDB rc8 (082dab3d) and rc7 (v1.2.0-rc7) in two alternating rounds of the full matrix
#    (both value sizes, pipeline 1 and 16, 3 measured runs per row per round)
#  - competitors, one round each, 100 B pipeline 16 only, between the KiviDB rounds
#  - the LPOP tail repro for rc8, once
# No memory pass (MEMPASS=0): this round checks throughput and latency only.
set -u
cd ~
log() { echo "$(date -u +%T) $*" | tee -a ~/master.log; }
run() { # engine run-name [env...]
  local e=$1 r=$2; shift 2
  log "START $r"
  env RUN="$r" MEMPASS=0 "$@" bash harness/datatypes.sh "$e" > ~/out/log-"$r".txt 2>&1
  grep -E "WARN|failed|ALL-DONE" ~/out/log-"$r".txt | tee -a ~/master.log
}
log "binaries: $(~/bin/kividb-rc8 --version) / $(~/bin/kividb-rc7 --version)"
run kividb-rc8 kividb-rc8
run kividb-rc7 kividb-rc7
run redis redis SIZES=100 ONLY_P=16
run dragonfly dragonfly SIZES=100 ONLY_P=16
run kividb-rc7 kividb-rc7-r2
run kividb-rc8 kividb-rc8-r2
run valkey valkey SIZES=100 ONLY_P=16
run redis-cluster redis-cluster SIZES=100 ONLY_P=16
log "matrix done"
echo "== kividb-rc8" > ~/lpop-tail.log
DEFRAG=yes bash harness/lpop-tail.sh kividb-rc8 >> ~/lpop-tail.log 2>&1
log "lpop-tail done"
python3 harness/summarize.py ~/out/datatypes ~/out/datatypes-summary | tee -a ~/master.log
log "MASTER-DONE"
