#!/usr/bin/env bash
# Three power-offs of pf1 under write load (rc7, AOF everysec, aof-ack-after-write yes = default):
# reps 1-2 az vm stop --skip-shutdown, rep 3 az vm deallocate.
W=$(cd "$(dirname "$0")" && pwd)
export RG=kividb-rc7-failover MASTER_VM=pf1 SSH_CLIENT="bash $W/s.sh pfc" RES_DIR=pl-results
P=$W/../rc7tests/failover/harness/pl_drive.sh
for rep in 1 2 3; do
  m=stop; [ $rep = 3 ] && m=deallocate
  echo "== rep $rep ($m) $(date -u +%T)"
  STOP_MODE=$m bash $P kividb power_loss_everysec $rep
done
echo PL-DONE $(date -u +%T)
