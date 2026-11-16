#!/usr/bin/env bash
# b2-build.sh: rc8 rebuilt (b1 was deleted after callgrind) for the replica-shutdown repro; pushed to the test VMs as /opt/kv/kividb-rc8.
set -u
SO="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"
bash ~/build.sh 082dab3d8929c180bccf993835e28992ea054fb5 rc8 > ~/build-rc8.out 2>&1
for h in 10.91.1.11 10.91.1.12 10.91.1.13 10.91.1.21 10.91.1.22 10.91.1.23; do
  scp -q $SO ~/kb-rc8/kividb $h:/opt/kv/kividb-rc8.new && ssh $SO $h 'mv /opt/kv/kividb-rc8.new /opt/kv/kividb-rc8; sha256sum /opt/kv/kividb-rc8' | sed "s/^/$h /"
done > ~/pushed.txt 2>&1
echo RC8-PUSHED >> ~/pushed.txt
