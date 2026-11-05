#!/usr/bin/env bash
# s.sh <vm-name|ip> <command...>: ssh to a VM from the laptop, retrying while ssh itself fails (255)
W=$(cd "$(dirname "$0")" && pwd)
h=$1; shift
ip=$(awk -v n="$h" '$1==n{print $2}' $W/vms.tsv); [ -z "$ip" ] && ip=$h
for i in 1 2 3 4 5 6 7 8; do
  ssh -i ${SSH_KEY:-$HOME/.ssh/id_ed25519} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ConnectTimeout=15 -o ServerAliveInterval=20 azureuser@$ip "$@"; rc=$?
  [ $rc != 255 ] && exit $rc; sleep 5
done; exit 255
