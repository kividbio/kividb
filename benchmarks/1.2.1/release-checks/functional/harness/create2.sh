#!/usr/bin/env bash
# create2.sh: RG kividb-c3-func2 (eastus2): replication set (eastus2: no D8s_v5 capacity; swedencentral: regional cores used by others; so southindia) (cl D8s_v5 client+sentinels, n1..n3 D8s_v5, 10.92.1.x)
# (southindia had no D8s_v5 capacity either: SIZE=Standard_D8s_v6) and cluster set (cl-c1..c3 D8s_v5 nodes, cl-cc D8s_v5 client, 10.82.1.x). 64 vCPU (regional limit 65).
set -u
RG=kividb-c3-func2 L=${L:-southindia}   # RG metadata in eastus2 (first try: no D8s_v5 capacity there)
KEY=<local path>
IP=<laptop ip>
IMG=Canonical:ubuntu-24_04-lts:server:latest
az group exists -n $RG | grep -q true || az group create -n $RG -l eastus2 -o none || exit 1
az network nsg create -g $RG -l $L -n nsg-$L -o none
az network nsg rule create -g $RG --nsg-name nsg-$L -n ssh --priority 100 --access Allow --protocol Tcp \
  --direction Inbound --source-address-prefixes $IP --destination-port-ranges 22 -o none
az network vnet create -g $RG -l $L -n vnet-$L --address-prefixes 10.92.0.0/16 10.82.0.0/16 --subnet-name s --subnet-prefix 10.92.1.0/24 -o none
az network vnet subnet create -g $RG --vnet-name vnet-$L -n c --address-prefix 10.82.1.0/24 -o none
az network vnet subnet update -g $RG --vnet-name vnet-$L -n s --network-security-group nsg-$L -o none
az network vnet subnet update -g $RG --vnet-name vnet-$L -n c --network-security-group nsg-$L -o none
mk() { az vm create -g $RG -l $L -n $1 --size ${SIZE:-Standard_D8s_v5} --image $IMG --admin-username azureuser --ssh-key-values "$KEY" \
  --vnet-name vnet-$L --subnet $2 --nsg "" --nsg-rule NONE --private-ip-address $3 --public-ip-sku Standard \
  --os-disk-size-gb 64 --accelerated-networking true -o none --only-show-errors && echo "made $1"; }
mk cl s 10.92.1.10 & mk n1 s 10.92.1.11 & mk n2 s 10.92.1.12 & mk n3 s 10.92.1.13 &
mk cl-c1 c 10.82.1.11 & mk cl-c2 c 10.82.1.12 & mk cl-c3 c 10.82.1.13 & mk cl-cc c 10.82.1.14 &
wait
T=$(date -u -d '+8 hours' +%H%M)
for v in cl n1 n2 n3 cl-c1 cl-c2 cl-c3 cl-cc; do az vm auto-shutdown -g $RG -n $v --time $T --location $L -o none; done
az vm list-ip-addresses -g $RG -o tsv --query "[].[virtualMachine.name,virtualMachine.network.publicIpAddresses[0].ipAddress]"
