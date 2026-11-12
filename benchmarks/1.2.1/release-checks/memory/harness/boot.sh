#!/bin/bash
# laptop-side: boot.sh <engine-public-ip> <client-public-ip> [build]
# copies harness, installs a client->engine ssh key, starts setup-common on both (+ setup-client, + build on the engine)
set -u
S=$1; C=$2; BUILD=${3:-}
KEY="<local path>"
SC="<local path>"
H=$SC/out/memory/harness
O="-i $KEY -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=20 -o LogLevel=ERROR"
for h in $S $C; do
  for i in $(seq 1 30); do ssh $O azureuser@$h true && break; sleep 10; done
  ssh $O azureuser@$h 'sudo mkdir -p /opt/mem && sudo chown azureuser /opt/mem'
  scp -q $O $H/*.sh $H/*.py $H/*.txt azureuser@$h:/opt/mem/
  ssh $O azureuser@$h 'chmod +x /opt/mem/*.sh'
done
ssh $O azureuser@$C 'test -f ~/.ssh/id_mem || ssh-keygen -q -t ed25519 -N "" -f ~/.ssh/id_mem; cat ~/.ssh/id_mem.pub' | ssh $O azureuser@$S 'cat >> ~/.ssh/authorized_keys'
ssh $O azureuser@$C 'nohup bash -c "sudo bash /opt/mem/setup-common.sh && sudo bash /opt/mem/setup-client.sh" > /var/tmp/setup.log 2>&1 < /dev/null &'
if [ -n "$BUILD" ]; then
  scp -q $O $SC/comb.tgz azureuser@$S:/var/tmp/comb.tgz
  ssh $O azureuser@$S 'nohup bash -c "sudo bash /opt/mem/setup-common.sh && bash /opt/mem/build-release.sh /var/tmp/comb.tgz" > /var/tmp/setup.log 2>&1 < /dev/null &'
else
  ssh $O azureuser@$S 'nohup sudo bash /opt/mem/setup-common.sh > /var/tmp/setup.log 2>&1 < /dev/null &'
fi
echo booted $S $C
