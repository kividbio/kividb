#!/usr/bin/env bash
# create4.sh: the replication set in RG kividb-c3-func2, westus2 (next to the cluster set): cl (client + Sentinels),
# n1..n3 (servers), Standard_D8s_v5 each, 10.92.1.10-13 in the same VNet (second address space). 64 vCPU total there.
set -u
RG=kividb-c3-func2 L=westus2 SIZE=${SIZE:-Standard_D8s_v5}
KEY=<local path>
IMG=Canonical:ubuntu-24_04-lts:server:latest
az network vnet update -g $RG -n vnet-$L --address-prefixes 10.82.0.0/16 10.92.0.0/16 -o none
az network vnet subnet create -g $RG --vnet-name vnet-$L -n s --address-prefix 10.92.1.0/24 --network-security-group nsg-$L -o none
mk() { az vm create -g $RG -l $L -n $1 --size $SIZE --image $IMG --admin-username azureuser --ssh-key-values "$KEY" \
  --vnet-name vnet-$L --subnet s --nsg "" --nsg-rule NONE --private-ip-address $2 --public-ip-sku Standard \
  --os-disk-size-gb 64 --accelerated-networking true -o none --only-show-errors 2>&1 | grep -oE "SkuNotAvailable|OperationNotAllowed|exceeding[^.]*" | head -1; az vm show -g $RG -n $1 -o none 2>/dev/null && echo "made $1"; }
mk cl 10.92.1.10 & mk n1 10.92.1.11 & mk n2 10.92.1.12 & mk n3 10.92.1.13 &
wait
T=$(date -u -d '+8 hours' +%H%M)
for v in cl n1 n2 n3; do az vm auto-shutdown -g $RG -n $v --time $T --location $L -o none; done
az vm list-ip-addresses -g $RG -o tsv --query "[].[virtualMachine.name,virtualMachine.network.publicIpAddresses[0].ipAddress]"
