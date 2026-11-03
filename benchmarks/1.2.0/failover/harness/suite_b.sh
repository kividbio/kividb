#!/usr/bin/env bash
# suite_b.sh: second, identical VM set (fo4-fo6 + fod, same size, same PPG) runs rep 3 of the 17 scenarios for both
# engines and kill9_checkpoint reps 1-3, while the first set (fo1-fo3 + foc) runs reps 1 and 2. Same order as fo_all.sh.
export RES=$HOME/fo-results
H=$HOME/fo/harness
SCEN="promote_graceful promote_abrupt kill9_everysec kill9_always kill9_noack sigstop partition partition_promote partition_promote_minrep wait_kill9_promote c_failover c_failover_force c_kill9 c_wait_kill9 c_sigstop c_partition c_partition_minrep"
for s in $SCEN; do
  for e in kividb redis; do
    [ "$e" = redis ] && [ "$s" = kill9_noack ] && continue
    timeout 400 bash "$H/fo_run.sh" "$e" "$s" 3 2>&1 | tail -1
  done
done
for r in 1 2 3; do timeout 400 bash "$H/fo_run.sh" kividb kill9_checkpoint $r 2>&1 | tail -1; done
echo SUITE-DONE
