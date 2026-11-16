#!/usr/bin/env bash
# create.sh <ssh-public-key-file>: RG kividb-rc7-soak in swedencentral: vnet, NSG (SSH from the operator's IP only),
# two proximity placement groups, seven VMs, auto-shutdown 7 h out on every one.
#   soak 1: r7-sm (master), r7-sr (replica): D8ds_v5 (local NVMe); r7-sc (client): D8s_v5
#   soak 2: r7-c1..r7-c3 (two nodes each): D8ds_v5; r7-cc (client): D8s_v5
set -u
RG=${RG:-kividb-rc7-soak} L=${L:-swedencentral}
K=$1
IP=$(curl -s https://api.ipify.org)
az group create -n $RG -l $L --tags purpose=rc7-soak -o none
az network vnet create -g $RG -n r7-vnet --address-prefix 10.70.0.0/16 --subnet-name s1 --subnet-prefix 10.70.1.0/24 -o none
az network nsg create -g $RG -n r7-nsg -o none
az network nsg rule create -g $RG --nsg-name r7-nsg -n ssh --priority 100 --source-address-prefixes "$IP/32" --destination-port-ranges 22 --access Allow --protocol Tcp -o none
az network vnet subnet update -g $RG --vnet-name r7-vnet -n s1 --network-security-group r7-nsg -o none
az ppg create -g $RG -n r7-ppg1 -l $L -o none
az ppg create -g $RG -n r7-ppg2 -l $L -o none
mk() { # name size ppg
  az vm create -g $RG -n $1 --size $2 --image Canonical:ubuntu-24_04-lts:server:latest --admin-username azureuser --ssh-key-values @$K \
    --vnet-name r7-vnet --subnet s1 --nsg "" --public-ip-sku Standard --os-disk-size-gb 128 \
    --storage-sku Premium_LRS --accelerated-networking true --ppg $3 -o none --only-show-errors 2>&1 | tail -3
  az vm auto-shutdown -g $RG -n $1 --time "$(date -u -d '+7 hours' +%H%M)" -o none
  echo "made $1"
}
mk r7-sm Standard_D8ds_v5 r7-ppg1 &
mk r7-sr Standard_D8ds_v5 r7-ppg1 &
mk r7-sc Standard_D8s_v5 r7-ppg1 &
mk r7-c1 Standard_D8ds_v5 r7-ppg2 &
mk r7-c2 Standard_D8ds_v5 r7-ppg2 &
mk r7-c3 Standard_D8ds_v5 r7-ppg2 &
mk r7-cc Standard_D8s_v5 r7-ppg2 &
wait
az vm list -g $RG -d --query "[].[name,publicIps,privateIps]" -o tsv
