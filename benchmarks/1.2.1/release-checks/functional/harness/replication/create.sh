#!/usr/bin/env bash
# create.sh: RG kividb-r121f-repl: cl (D16s_v5: build, client, 3 Sentinels), n1..n3 (D8s_v5: servers).
# One VNet, NSG with one rule: SSH from the laptop /32. Auto-shutdown ~6 h out.
set -u
RG=kividb-r121f-repl L=${L:-swedencentral}
KEY=<local path>
IP=<laptop ip>
IMG=Canonical:ubuntu-24_04-lts:server:latest
az group exists -n $RG | grep -q true || az group create -n $RG -l $L -o none || exit 1
az network vnet create -g $RG -l $L -n vnet --address-prefix 10.92.0.0/16 --subnet-name s --subnet-prefix 10.92.1.0/24 -o none
az network nsg create -g $RG -l $L -n nsg -o none
az network nsg rule create -g $RG --nsg-name nsg -n ssh --priority 100 --access Allow --protocol Tcp \
  --direction Inbound --source-address-prefixes $IP --destination-port-ranges 22 -o none
az network vnet subnet update -g $RG --vnet-name vnet -n s --network-security-group nsg -o none
mk() { az vm create -g $RG -l $L -n $1 --size $2 --image $IMG --admin-username azureuser --ssh-key-values "$KEY" \
  --vnet-name vnet --subnet s --nsg "" --nsg-rule NONE --private-ip-address $3 --public-ip-sku Standard \
  --os-disk-size-gb 64 --accelerated-networking true -o none --only-show-errors && echo "made $1"; }
mk cl Standard_D16s_v5 10.92.1.10 &
mk n1 Standard_D8s_v5 10.92.1.11 &
mk n2 Standard_D8s_v5 10.92.1.12 &
mk n3 Standard_D8s_v5 10.92.1.13 &
wait
T=$(date -u -d '+6 hours' +%H%M)
for v in cl n1 n2 n3; do az vm auto-shutdown -g $RG -n $v --time $T -o none; done
az vm list-ip-addresses -g $RG -o tsv --query "[].[virtualMachine.name,virtualMachine.network.publicIpAddresses[0].ipAddress]"
