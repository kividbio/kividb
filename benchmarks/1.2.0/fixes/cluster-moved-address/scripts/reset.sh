#!/usr/bin/env bash
# Run with sudo: clear leftover servers, namespaces and veths; clear old e2e output.
pkill -9 -x kividb
for i in 1 2 3 4 5 6; do ip link del vh$i 2>/dev/null; ip netns del n$i 2>/dev/null; done
ip link del clmbr 2>/dev/null
rm -rf /home/azureuser/out/e2e /clm
ip -br link | grep -E "vh|clm" || echo "net clean"
ip netns
