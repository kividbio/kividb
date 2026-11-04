#!/usr/bin/env bash
# create.sh <ssh-public-key-file>: RG kividb-rc8-soak in swedencentral (fallback japaneast): vnet, NSG (SSH from the operator's IP only),
# two proximity placement groups, seven VMs, auto-shutdown 7 h out on every one.
#   soak 1: r8-sm (master), r8-sr (replica): D8ds_v5 (local NVMe); r8-sc (client): D8s_v5
#   soak 2: r8-c1..r8-c3 (two nodes each): D8ds_v5; r8-cc (client): D8s_v5
set -u
RG=${RG:-kividb-rc8-soak} L=${L:-swedencentral}
K=$1
IP=$(curl -s https://api.ipify.org)
az group create -n $RG -l $L --tags purpose=rc8-soak -o none
az network vnet create -g $RG -n r8-vnet --address-prefix 10.80.0.0/16 --subnet-name s1 --subnet-prefix 10.80.1.0/24 -o none
az network nsg create -g $RG -n r8-nsg -o none
az network nsg rule create -g $RG --nsg-name r8-nsg -n ssh --priority 100 --source-address-prefixes "$IP/32" --destination-port-ranges 22 --access Allow --protocol Tcp -o none
az network vnet subnet update -g $RG --vnet-name r8-vnet -n s1 --network-security-group r8-nsg -o none
az ppg create -g $RG -n r8-ppg1 -l $L -o none
az ppg create -g $RG -n r8-ppg2 -l $L -o none
mk() { # name size ppg
  az vm create -g $RG -n $1 --size $2 --image Canonical:ubuntu-24_04-lts:server:latest --admin-username azureuser --ssh-key-values @$K \
    --vnet-name r8-vnet --subnet s1 --nsg "" --public-ip-sku Standard --os-disk-size-gb 128 \
    --storage-sku Premium_LRS --accelerated-networking true --ppg $3 -o none --only-show-errors 2>&1 | tail -3
  az vm auto-shutdown -g $RG -n $1 --time "$(date -u -d '+7 hours' +%H%M)" -o none
  echo "made $1"
}
mk r8-sm Standard_D8ds_v5 r8-ppg1 &
mk r8-sr Standard_D8ds_v5 r8-ppg1 &
mk r8-sc Standard_D8s_v5 r8-ppg1 &
mk r8-c1 Standard_D8ds_v5 r8-ppg2 &
mk r8-c2 Standard_D8ds_v5 r8-ppg2 &
mk r8-c3 Standard_D8ds_v5 r8-ppg2 &
mk r8-cc Standard_D8s_v5 r8-ppg2 &
wait
az vm list -g $RG -d --query "[].[name,publicIps,privateIps]" -o tsv
