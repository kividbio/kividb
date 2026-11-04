#!/usr/bin/env bash
# pl2.sh: two power-offs of pf1 under write load (rc8, AOF everysec, aof-ack-after-write yes), az vm stop --skip-shutdown,
# with the fixed pl_drive.sh (retries while ssh exits 255; waits until no fo_run.sh runs).
W=$(cd "$(dirname "$0")" && pwd)
export RG=kividb-rc8-failover MASTER_VM=pf1 SSH_CLIENT="bash $W/s.sh pfc" RES_DIR=pl-results
P=$W/pl_drive.sh
for rep in 1 2; do
  echo "== rep $rep (stop --skip-shutdown) $(date -u +%T)"
  STOP_MODE=stop bash $P kividb power_loss_everysec $rep
done
echo PL-DONE $(date -u +%T)
