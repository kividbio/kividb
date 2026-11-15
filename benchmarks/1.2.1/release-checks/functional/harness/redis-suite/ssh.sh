#!/usr/bin/env bash
# ssh.sh <u|t> <command...>   (u = rs-uring VM, t = rs-tokio VM)
S="<local path>"
case $1 in u) IP=<vm ip>;; t) IP=<vm ip>;; esac; shift
exec ssh -i "$S/cloud/az-perf-key" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ConnectTimeout=15 -o ServerAliveInterval=20 azureuser@$IP "$@"
