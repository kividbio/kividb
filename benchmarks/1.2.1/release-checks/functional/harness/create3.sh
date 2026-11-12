#!/usr/bin/env bash
# create3.sh: the cluster set in RG kividb-c3-func2 (RG metadata eastus2): cl-c1..c3 (nodes), cl-cc (client), 10.82.1.x.
# Region L tried in turn until one has capacity (eastus2, swedencentral, southindia failed for D8s_v5 / regional cores).
# Same VM size as the earlier cluster pass (Standard_D8s_v5).
set -u
RG=kividb-c3-func2 L=$1 SIZE=${SIZE:-Standard_D8s_v5}
KEY=<local path>
IP=<laptop ip>
IMG=Canonical:ubuntu-24_04-lts:server:latest
az network nsg create -g $RG -l $L -n nsg-$L -o none
az network nsg rule create -g $RG --nsg-name nsg-$L -n ssh --priority 100 --access Allow --protocol Tcp \
  --direction Inbound --source-address-prefixes $IP --destination-port-ranges 22 -o none
az network vnet create -g $RG -l $L -n vnet-$L --address-prefixes 10.82.0.0/16 --subnet-name c --subnet-prefix 10.82.1.0/24 -o none
az network vnet subnet update -g $RG --vnet-name vnet-$L -n c --network-security-group nsg-$L -o none
mk() { az vm create -g $RG -l $L -n $1 --size $SIZE --image $IMG --admin-username azureuser --ssh-key-values "$KEY" \
  --vnet-name vnet-$L --subnet c --nsg "" --nsg-rule NONE --private-ip-address $2 --public-ip-sku Standard \
  --os-disk-size-gb 64 --accelerated-networking true -o none --only-show-errors 2>&1 | grep -oE "SkuNotAvailable|OperationNotAllowed|exceeding[^.]*" | head -1; az vm show -g $RG -n $1 -o none 2>/dev/null && echo "made $1"; }
mk cl-cc 10.82.1.14 | grep -q "made" || { echo "NO CAPACITY in $L"; az network vnet delete -g $RG -n vnet-$L; az network nsg delete -g $RG -n nsg-$L; exit 1; }
mk cl-c1 10.82.1.11 & mk cl-c2 10.82.1.12 & mk cl-c3 10.82.1.13 &
wait
T=$(date -u -d '+8 hours' +%H%M)
for v in cl-c1 cl-c2 cl-c3 cl-cc; do az vm auto-shutdown -g $RG -n $v --time $T --location $L -o none; done
az vm list-ip-addresses -g $RG -o tsv --query "[].[virtualMachine.name,virtualMachine.network.publicIpAddresses[0].ipAddress]"
