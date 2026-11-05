#!/usr/bin/env bash
# b1-build.sh: rc9 then rc8 (build.sh), each binary pushed to the test VMs as soon as it is built
# (rc9 as /opt/kv/kividb; rc8 stays on b1 for callgrind).
set -u
SO="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"
bash ~/build.sh 4a47644e53b8cf261df10309d919f2c19ebadef0 rc9 > ~/build-rc9.out 2>&1
for h in 10.91.1.11 10.91.1.12 10.91.1.13 10.91.1.21 10.91.1.22 10.91.1.23; do
  scp -q $SO ~/kb-rc9/kividb $h:/opt/kv/kividb.new && ssh $SO $h 'mv /opt/kv/kividb.new /opt/kv/kividb; sha256sum /opt/kv/kividb' | sed "s/^/$h /"
done > ~/pushed.txt 2>&1
echo RC9-PUSHED >> ~/pushed.txt
bash ~/build.sh 082dab3d8929c180bccf993835e28992ea054fb5 rc8 > ~/build-rc8.out 2>&1
echo BUILDS-DONE > ~/builds-done.txt
