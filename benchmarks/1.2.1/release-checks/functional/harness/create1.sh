#!/usr/bin/env bash
# create1.sh: RG kividb-c3-func (centralindia): fp (D16ds_v5 + P30 data disk: build + persistence),
# pcli (D4s_v5: power-loss client), fe (D8s_v5: expiry re-test + snapshot compat), fs (D16s_v5: Redis Tcl suite).
set -u
RG=kividb-c3-func L=centralindia
KEY=<local path>
IP=<laptop ip>
IMG=Canonical:ubuntu-24_04-lts:server:latest
az group create -n $RG -l $L -o none || exit 1
az network vnet create -g $RG -l $L -n vnet --address-prefix 10.0.0.0/16 --subnet-name s --subnet-prefix 10.0.0.0/24 -o none
az network nsg create -g $RG -l $L -n nsg -o none
az network nsg rule create -g $RG --nsg-name nsg -n ssh --priority 100 --access Allow --protocol Tcp \
  --direction Inbound --source-address-prefixes $IP --destination-port-ranges 22 -o none
az network vnet subnet update -g $RG --vnet-name vnet -n s --network-security-group nsg -o none
mk() { az vm create -g $RG -l $L -n $1 --size $2 --image $IMG --admin-username azureuser --ssh-key-values "$KEY" \
  --vnet-name vnet --subnet s --nsg "" --nsg-rule NONE --private-ip-address $3 --public-ip-sku Standard \
  --os-disk-size-gb 128 --accelerated-networking true -o none --only-show-errors "${@:4}" && echo "made $1"; }
mk fp Standard_D16ds_v5 10.0.0.4 --data-disk-sizes-gb 1024 --storage-sku os=Premium_LRS 0=Premium_LRS --data-disk-caching None &
mk pcli Standard_D4s_v5 10.0.0.5 &
mk fe Standard_D8s_v5 10.0.0.6 &
mk fs Standard_D16s_v5 10.0.0.7 &
wait
T=$(date -u -d '+8 hours' +%H%M)
for v in fp pcli fe fs; do az vm auto-shutdown -g $RG -n $v --time $T -o none; done
az vm list-ip-addresses -g $RG -o tsv --query "[].[virtualMachine.name,virtualMachine.network.publicIpAddresses[0].ipAddress]"
