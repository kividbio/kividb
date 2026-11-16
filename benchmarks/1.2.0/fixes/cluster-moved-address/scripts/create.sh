#!/usr/bin/env bash
# Create RG kividb-rc4-clmove with one VM (SSH from laptop only), auto-shutdown.
set -u
RG=kividb-rc4-clmove L=${L:-centralindia} SIZE=${SIZE:-Standard_D8s_v5}
PUB="${SSH_PUBKEY:?set SSH_PUBKEY to the public key to install on the VM}"
IP=$(curl -s https://api.ipify.org)
echo "laptop ip $IP"
az group create -n $RG -l $L -o none
az network nsg create -g $RG -n clm-nsg -l $L -o none
az network nsg rule create -g $RG --nsg-name clm-nsg -n ssh --priority 100 --source-address-prefixes "$IP/32" --destination-port-ranges 22 --access Allow --protocol Tcp -o none
az vm create -g $RG -n clm-vm -l $L --size "$SIZE" --image Canonical:ubuntu-24_04-lts:server:latest \
  --admin-username azureuser --ssh-key-values "$PUB" --nsg clm-nsg \
  --public-ip-sku Standard --os-disk-size-gb 128 --storage-sku Premium_LRS -o none --only-show-errors 2>&1 | tail -5
az vm auto-shutdown -g $RG -n clm-vm --time "${SHUT:-0300}" -o none
az vm list-ip-addresses -g $RG --query "[].virtualMachine.network.publicIpAddresses[0].ipAddress" -o tsv
