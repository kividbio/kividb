#!/usr/bin/env bash
# create.sh: RG kividb-r121f-cluster in australiaeast (env L= for fallback): vnet, NSG (SSH from <laptop ip> only),
# 4 x Standard_D8s_v5: c1..c3 (two KiviDB nodes each), cc (client + build). Auto-shutdown 6 h out.
set -u
RG=${RG:-kividb-r121f-cluster} L=${L:-australiaeast}
K=$1
IP=<laptop ip>
SD=$(date -u -d '+6 hours' +%H%M)
az group create -n $RG -l $L --tags purpose=r121f-cluster-test -o none
az network vnet create -g $RG -n cl-vnet --address-prefix 10.82.0.0/16 --subnet-name s1 --subnet-prefix 10.82.1.0/24 -o none
az network nsg create -g $RG -n cl-nsg -o none
az network nsg rule create -g $RG --nsg-name cl-nsg -n ssh --priority 100 --source-address-prefixes "$IP/32" --destination-port-ranges 22 --access Allow --protocol Tcp -o none
az network vnet subnet update -g $RG --vnet-name cl-vnet -n s1 --network-security-group cl-nsg -o none
mk() { # name size private-ip
  out=$(az vm create -g $RG -n $1 --size $2 --image Canonical:ubuntu-24_04-lts:server:latest --admin-username azureuser --ssh-key-values @$K \
    --vnet-name cl-vnet --subnet s1 --nsg-rule NONE --nsg "" --private-ip-address $3 --public-ip-sku Standard --os-disk-size-gb 64 \
    --storage-sku Premium_LRS --accelerated-networking true -o tsv --query publicIpAddress --only-show-errors 2>&1)
  if echo "$out" | grep -q -E 'ERROR|Error'; then echo "FAIL $1 $2: $(echo "$out" | grep -v '^$' | tail -2 | tr '\n' ' ')"; return; fi
  az vm auto-shutdown -g $RG -n $1 --time "$SD" -o none && echo "made $1 (auto-shutdown $SD UTC)"
}
mk cl-c1 Standard_D8s_v5 10.82.1.11 &
mk cl-c2 Standard_D8s_v5 10.82.1.12 &
mk cl-c3 Standard_D8s_v5 10.82.1.13 &
mk cl-cc Standard_D8s_v5 10.82.1.14 &
wait
az vm list -g $RG -d --query "[].[name,publicIps,privateIps]" -o tsv
