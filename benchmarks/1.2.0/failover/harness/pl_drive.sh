#!/usr/bin/env bash
# pl_drive.sh <engine> <power_loss_always|power_loss_everysec> <rep>
# Runs on the operator's machine (needs az and ssh to the client VM). Starts the scenario on the client VM,
# powers the master's VM off without an OS shutdown when the scenario asks (az vm stop --skip-shutdown),
# powers it on again, and tells the scenario it is back.
#   env: RG, MASTER_VM (default ops-fo1), SSH_CLIENT (command prefix that runs a command on the client VM)
set -u
E=$1 S=$2 REP=$3
RG=${RG:-kividb-rc4-ops}
VM=${MASTER_VM:-ops-fo1}
C=${SSH_CLIENT:?set SSH_CLIENT, e.g. "ssh azureuser@<client-ip>"}
RESD=${RES_DIR:-fo-results}
D=$RESD/$E/$S/$REP
$C "RES=$RESD nohup bash fo/harness/fo_run.sh $E $S $REP > fo-pl-$E-$S-$REP.log 2>&1 < /dev/null &"
for _ in $(seq 1 300); do $C "test -f $D/poweroff-now" && break; sleep 1; done
t=$(date +%s.%N)
az vm stop -g "$RG" -n "$VM" --skip-shutdown -o none
$C "echo $t > $D/poweroff-at"
az vm start -g "$RG" -n "$VM" -o none
$C "touch $D/power-back"
sleep 5
for _ in $(seq 1 600); do $C "pgrep -f '[f]o_run.sh $E $S' >/dev/null" || break; sleep 5; done
$C "tail -1 fo-pl-$E-$S-$REP.log"
