#!/bin/bash
# collect.sh <name> <engine-ip> <client-ip> : copies client /var/tmp/mem + /var/tmp/wf* and engine info into coll/<name>
set -u
N=$1; S=$2; C=$3
KEY="<local path>"
O="-i $KEY -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=20 -o LogLevel=ERROR"
D="<local path>"
mkdir -p "$D"
ssh $O azureuser@$C 'cd /var/tmp && rm -f /var/tmp/mem/dels-*.resp; tar czf /var/tmp/cli.tgz mem $(ls -d wf wf-*.log 2>/dev/null) kernel.txt lscpu.txt'
scp -q $O azureuser@$C:/var/tmp/cli.tgz "$D/cli.tgz"
ssh $O azureuser@$S 'cd /var/tmp && tar czf /var/tmp/srv.tgz mem *-version *-sha256 *-tarball-check *-image engine-images.txt kernel.txt lscpu.txt free.txt 2>/dev/null; true'
scp -q $O azureuser@$S:/var/tmp/srv.tgz "$D/srv.tgz"
ls -la "$D"
