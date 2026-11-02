#!/usr/bin/env bash
# up.sh SSH_SOURCE_IP [PUBKEY]: the two VMs these suites ran on.
#   srv  Standard_D16ds_v5  (16 vCPU Intel Xeon Platinum 8370C, 64 GB, local NVMe temp disk)
#   cli  Standard_D48s_v5   (48 vCPU, 192 GB): the load generator, 3x the server's vCPUs
# Same zone and the same proximity placement group, one VNet, accelerated
# networking (the default for these sizes). SSH only from SSH_SOURCE_IP,
# auto-shutdown as a backstop. Delete with: az group delete -n $RG --yes
set -eu
RG=${RG:-kividb-rc4-types} REGION=${REGION:-koreacentral} ZONE=${ZONE:-1} HOURS=${HOURS:-12}
IP=$1 PUB=${2:-$HOME/.ssh/id_rsa.pub}
az group create -n "$RG" -l "$REGION" --tags purpose=kividb-rc4-types -o none
az ppg create -g "$RG" -n ppg -l "$REGION" --zone "$ZONE" -t Standard \
  --intent-vm-sizes Standard_D16ds_v5 Standard_D48s_v5 -o none
for spec in srv:Standard_D16ds_v5 cli:Standard_D48s_v5; do
  NAME=${spec%%:*} SIZE=${spec#*:}
  az vm create -g "$RG" -n "$NAME" --size "$SIZE" --zone "$ZONE" --ppg ppg \
    --image Canonical:ubuntu-24_04-lts:server:latest \
    --admin-username azureuser --ssh-key-values "$PUB" --public-ip-sku Standard \
    --vnet-name vnet --subnet sub --nsg-rule NONE --os-disk-size-gb 256 --storage-sku Premium_LRS \
    --accelerated-networking true -o none
  az network nsg rule create -g "$RG" --nsg-name "${NAME}NSG" -n ssh-in --priority 100 \
    --source-address-prefixes "$IP" --destination-port-ranges 22 --access Allow --protocol Tcp -o none
  az vm auto-shutdown -g "$RG" -n "$NAME" --time "$(date -u -d "+$HOURS hours" +%H%M)" -o none
done
az vm list-ip-addresses -g "$RG" --query "[].{n:virtualMachine.name,pub:virtualMachine.network.publicIpAddresses[0].ipAddress,priv:virtualMachine.network.privateIpAddresses[0]}" -o tsv
