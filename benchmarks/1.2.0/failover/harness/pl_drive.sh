#!/usr/bin/env bash
# pl_drive.sh <engine> <power_loss_always|power_loss_everysec> <rep>
# Runs on the operator's machine (needs az and ssh to the client VM). Starts the scenario on the client VM,
# powers the master's VM off without an OS shutdown when the scenario asks (az vm stop --skip-shutdown),
# powers it on again, and tells the scenario it is back.
#   env: RG, MASTER_VM (default ops-fo1), SSH_CLIENT (command prefix that runs a command on the client VM)
#        STOP_MODE=stop (default: az vm stop --skip-shutdown) | deallocate (az vm deallocate, a hard deallocate)
#
# rc7 change: every remote call retries while ssh itself fails (exit 255). Before, the wait loop at the end
# read an ssh failure as "the scenario has finished", so the next rep could start while this one still ran
# and wipe its server (seen on rc5). Before starting, it also waits until no fo_run.sh is running.
set -u
E=$1 S=$2 REP=$3
RG=${RG:-kividb-rc4-ops}
VM=${MASTER_VM:-ops-fo1}
C=${SSH_CLIENT:?set SSH_CLIENT, e.g. "ssh azureuser@<client-ip>"}
RESD=${RES_DIR:-fo-results}
MODE=${STOP_MODE:-stop}
D=$RESD/$E/$S/$REP
# r <remote command>: run on the client, retrying for up to ~10 min while ssh exits 255; returns the command's status
r() {
  local i rc
  for i in $(seq 1 120); do
    $C "$1"; rc=$?
    [ $rc != 255 ] && return $rc
    echo "pl_drive: ssh failed (255), retry $i" >&2; sleep 5
  done
  return 255
}
while r "pgrep -f '[f]o_run.sh' >/dev/null"; do sleep 5; done
r "RES=$RESD nohup bash fo/harness/fo_run.sh $E $S $REP > fo-pl-$E-$S-$REP.log 2>&1 < /dev/null &"
for _ in $(seq 1 300); do r "test -f $D/poweroff-now" && break; sleep 1; done
t=$(date +%s.%N)
if [ "$MODE" = deallocate ]; then
  az vm deallocate -g "$RG" -n "$VM" -o none
else
  az vm stop -g "$RG" -n "$VM" --skip-shutdown -o none
fi
r "echo $t > $D/poweroff-at"
az vm start -g "$RG" -n "$VM" -o none
r "touch $D/power-back"
sleep 5
while r "pgrep -f '[f]o_run.sh $E $S' >/dev/null"; do sleep 5; done
r "tail -1 fo-pl-$E-$S-$REP.log"
