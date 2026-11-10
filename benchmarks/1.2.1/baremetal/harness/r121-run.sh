#!/usr/bin/env bash
# The 1.2.1 bare-metal rerun of the 1.2.0 capture (results/2026-11-04-c7gn.metal-1.2.0),
# driven from a workstation like scripts/metal-120-run.sh. Each round runs under nohup on the
# load generator and is polled, so a dropped workstation link cannot cut a round short.
# Same method: server confined to 32 cores, 10 million keys of 100 bytes, 30 s a scenario,
# pipeline 1 and 16, write-only and read-only, two rounds an engine. Redis Cluster not run.
# KiviDB 1.2.1 and 1.2.0 run in the order 1.2.1, 1.2.0, (others), 1.2.0, 1.2.1, two rounds
# each time, so drift over the session favours neither build.
#
#   scripts/r121-run.sh <ssh-key> <server-public> <server-private> <client-public> <out-dir>
set -uo pipefail
KEY=$1 SERVER_PUB=$2 SERVER_PRIV=$3 CLIENT_PUB=$4 OUT=$5
mkdir -p "$OUT"
SSH=(ssh -i "$KEY" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=15 -o ServerAliveInterval=15 -o LogLevel=ERROR)
srv() { "${SSH[@]}" "ubuntu@$SERVER_PUB" "$@"; }
cli() { "${SSH[@]}" "ubuntu@$CLIENT_PUB" "$@"; }
log() { echo "[$(date -u +%T)] $*"; }
retry() { for _ in 1 2 3 4 5 6; do "$@" && return 0; sleep 10; done; return 1; }

cli "mkdir -p ~/results"

run_engine() { # label engine bin rounds...
  local label=$1 engine=$2 bin=$3
  shift 3
  log "start $label ($engine, BIN=$bin)"
  retry srv "CORES=32 PORT=6379 BIN=$bin ~/kivi-benchmark/scripts/start-server.sh $engine 2>&1 | tail -3"
  for round in "$@"; do
    local r=$label-r$round
    log "$r"
    retry cli "rm -f ~/$r.done; nohup bash -c 'OUT=~/results/$r DURATION=30 SIZES=100 PIPELINES=\"1 16\" RATIOS=\"1:0 0:1\" ~/kivi-benchmark/scripts/run-matrix.sh $r $SERVER_PRIV 6379 > ~/results-$r.log 2>&1; touch ~/$r.done' > /dev/null 2>&1 < /dev/null &"
    sleep 60
    until cli "test -f ~/$r.done" 2>/dev/null; do sleep 15; done
    retry cli "grep -a -E '^(== d|Totals)' ~/results-$r.log | tr -s ' ' | cut -d' ' -f1-3 | paste -d' ' - -"
  done
}

run_engine kividb121 kividb '$HOME/bin121' 1 2
run_engine kividb120 kividb '$HOME/bin' 1 2
run_engine garnet garnet '$HOME/bin' 1 2
run_engine dragonfly dragonfly '$HOME/bin' 1 2
run_engine redis redis '$HOME/bin' 1 2
run_engine kividb120 kividb '$HOME/bin' 3 4
run_engine kividb121 kividb '$HOME/bin121' 3 4
retry srv "~/kivi-benchmark/scripts/stop-server.sh; cat ~/logs/launch.log" > "$OUT/launch.log"
retry cli "tar -czf - -C ~ results" > "$OUT/client-results.tgz"
log "done"
