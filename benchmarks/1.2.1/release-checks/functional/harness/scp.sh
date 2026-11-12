#!/usr/bin/env bash
# scp.sh <scp args...>: laptop-side scp with the test key.
K=<local path>
timeout ${TO:-600} scp -q -i $K -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ConnectTimeout=15 -o BatchMode=yes "$@"
