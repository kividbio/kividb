#!/usr/bin/env bash
# ssh.sh <host-ip> <cmd...>: laptop-side ssh, bounded (TO, default 60 s), retried on exit 255 (max 4 tries).
K=<local path>
h=$1; shift
for i in 1 2 3 4; do
  timeout ${TO:-60} ssh -i $K -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ConnectTimeout=15 -o BatchMode=yes azureuser@$h "$@"
  rc=$?; [ $rc != 255 ] && exit $rc; sleep 5
done
exit 255
