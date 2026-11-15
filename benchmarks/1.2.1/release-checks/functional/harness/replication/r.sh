#!/usr/bin/env bash
# r.sh <cmd...>: run on the client VM (laptop side helper). r.sh -bg <cmd> starts it detached.
K=<local path>
SO="-i $K -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ConnectTimeout=15"
if [ "$1" = -bg ]; then shift; timeout 20 ssh -f $SO azureuser@<vm ip> "cd ~/h && nohup bash -c '$*' > /dev/null 2>&1 < /dev/null &"; echo "bg rc=$?"
else timeout ${TO:-60} ssh $SO azureuser@<vm ip> "$*"; fi
