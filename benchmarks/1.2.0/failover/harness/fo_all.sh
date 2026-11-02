#!/usr/bin/env bash
# fo_all.sh "<engines>" "<scenarios>" <reps>   e.g. fo_all.sh "kividb redis" "kill9_everysec c_kill9" 3
H=$(cd "$(dirname "$0")" && pwd)
ENGINES=${1:-"kividb redis"}
SCEN=${2:-"promote_graceful promote_abrupt kill9_everysec kill9_always kill9_noack sigstop partition partition_promote partition_promote_minrep wait_kill9_promote c_failover c_failover_force c_kill9 c_wait_kill9 c_sigstop c_partition c_partition_minrep"}
REPS=${3:-1}
for r in $(seq 1 "$REPS"); do
  for s in $SCEN; do
    for e in $ENGINES; do
      [ "$e" = redis ] && [ "$s" = kill9_noack ] && continue   # a KiviDB setting
      timeout 400 bash "$H/fo_run.sh" "$e" "$s" "$r" 2>&1 | tail -1
    done
  done
done
echo FO-ALL-DONE
