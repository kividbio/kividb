#!/usr/bin/env bash
# ssh.sh <host-ip> <cmd...>: laptop-side ssh with a bounded timeout (TO, default 60 s)
K=<local path>
h=$1; shift
timeout ${TO:-60} ssh -i $K -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ConnectTimeout=15 azureuser@$h "$@"
