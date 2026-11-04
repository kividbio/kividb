#!/usr/bin/env bash
# create.sh <ssh-public-key-file>: RG kividb-rc10-soak in swedencentral (env L=westus2 for the fallback): vnet, NSG
# (SSH from the operator's IP only), one proximity placement group, four VMs, auto-shutdown 7 h out on every one.
#   r10-c1..r10-c3 (two cluster nodes each): Standard_D8ds_v5, data on the local NVMe disk
#   r10-cc (cluster client, build): Standard_D8s_v5
# The rc8 soak's create.sh, cut to the cluster VMs.
set -u
RG=${RG:-kividb-rc10-soak} L=${L:-swedencentral}
K=$1
IP=$(curl -s https://api.ipify.org)
SD=$(date -u -d '+7 hours' +%H%M)
az group create -n $RG -l $L --tags purpose=rc10-soak -o none
az network vnet create -g $RG -n r10-vnet --address-prefix 10.81.0.0/16 --subnet-name s1 --subnet-prefix 10.81.1.0/24 -o none
az network nsg create -g $RG -n r10-nsg -o none
az network nsg rule create -g $RG --nsg-name r10-nsg -n ssh --priority 100 --source-address-prefixes "$IP/32" --destination-port-ranges 22 --access Allow --protocol Tcp -o none
az network vnet subnet update -g $RG --vnet-name r10-vnet -n s1 --network-security-group r10-nsg -o none
az ppg create -g $RG -n r10-ppg -l $L -o none
mk() { # name size private-ip
  out=$(az vm create -g $RG -n $1 --size $2 --image Canonical:ubuntu-24_04-lts:server:latest --admin-username azureuser --ssh-key-values @$K \
    --vnet-name r10-vnet --subnet s1 --nsg "" --private-ip-address $3 --public-ip-sku Standard --os-disk-size-gb 128 \
    --storage-sku Premium_LRS --accelerated-networking true --ppg r10-ppg -o tsv --query publicIpAddress --only-show-errors 2>&1)
  if echo "$out" | grep -q -E 'ERROR|Error'; then echo "FAIL $1 $2: $(echo "$out" | grep -v '^$' | tail -2 | tr '\n' ' ')"; return; fi
  az vm auto-shutdown -g $RG -n $1 --time "$SD" -o none && echo "made $1 (auto-shutdown $SD UTC)"
}
mk r10-c1 Standard_D8ds_v5 10.81.1.11 &
mk r10-c2 Standard_D8ds_v5 10.81.1.12 &
mk r10-c3 Standard_D8ds_v5 10.81.1.13 &
mk r10-cc Standard_D8s_v5 10.81.1.14 &
wait
az vm list -g $RG -d --query "[].[name,publicIps,privateIps]" -o tsv
