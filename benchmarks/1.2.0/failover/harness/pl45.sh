#!/usr/bin/env bash
# Two more power-offs (reps 4-5, stop --skip-shutdown) with the AOF tail recorded before the restart
W=$(cd "$(dirname "$0")" && pwd)
export RG=kividb-rc7-failover MASTER_VM=pf1 SSH_CLIENT="bash $W/s.sh pfc" RES_DIR=pl-results
P=$W/../rc7tests/failover/harness/pl_drive.sh
for rep in 4 5; do
  m=stop
  echo "== rep $rep ($m) $(date -u +%T)"
  STOP_MODE=$m bash $P kividb power_loss_everysec $rep
done
echo PL-DONE $(date -u +%T)
