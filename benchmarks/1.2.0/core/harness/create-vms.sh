#!/usr/bin/env bash
# Create the three machines of the core benchmark on Azure: one server and two
# load generators, all arm64, in one proximity placement group (one
# datacenter), with accelerated networking.
#
#   RG=kividb-rc4-core LOCATION=eastasia SSH_PUBLIC_KEY_FILE=~/.ssh/id.pub \
#     SSH_CIDR=203.0.113.7/32 bash create-vms.sh
#
# Machines (vCPU quota needed: 64):
#   srv   Standard_D32ps_v5  32 vCPU Ampere Altra, 128 GB, 16 Gbps   (server)
#   cl1   Standard_D16ps_v5  16 vCPU, 64 GB, 12.5 Gbps              (memtier)
#   cl2   Standard_D16ps_v5  16 vCPU, 64 GB, 12.5 Gbps              (memtier)
# Auto-shutdown is set SHUTDOWN_HOURS (default 8) from now as a backstop.
# Delete everything with: az group delete -n "$RG" --yes
set -euo pipefail

RG=${RG:?resource group}
LOCATION=${LOCATION:-eastasia}
SSH_PUBLIC_KEY_FILE=${SSH_PUBLIC_KEY_FILE:?public key file}
SSH_CIDR=${SSH_CIDR:?your ip/32 for ssh}
SERVER_SIZE=${SERVER_SIZE:-Standard_D32ps_v5}
CLIENT_SIZE=${CLIENT_SIZE:-Standard_D16ps_v5}
IMAGE=${IMAGE:-Canonical:ubuntu-24_04-lts:server-arm64:latest}
SHUTDOWN_HOURS=${SHUTDOWN_HOURS:-8}

az group create -n "$RG" -l "$LOCATION" -o none
az ppg create -n ppg -g "$RG" -l "$LOCATION" -t standard \
  --intent-vm-sizes "$SERVER_SIZE" "$CLIENT_SIZE" -o none
az network nsg create -g "$RG" -n nsg -o none
az network nsg rule create -g "$RG" --nsg-name nsg -n ssh --priority 100 \
  --source-address-prefixes "$SSH_CIDR" --destination-port-ranges 22 \
  --access Allow --protocol Tcp -o none
az network vnet create -g "$RG" -n vnet --address-prefix 10.40.0.0/16 \
  --subnet-name sub --subnet-prefix 10.40.0.0/24 -o none
az network vnet subnet update -g "$RG" --vnet-name vnet -n sub \
  --network-security-group nsg -o none

when=$(date -u -d "+${SHUTDOWN_HOURS} hours" +%H%M 2>/dev/null || date -u -v+"${SHUTDOWN_HOURS}"H +%H%M)
create() { # name size disk-gb private-ip
  az vm create -g "$RG" -n "$1" --size "$2" --image "$IMAGE" \
    --admin-username azureuser --ssh-key-values "$SSH_PUBLIC_KEY_FILE" \
    --vnet-name vnet --subnet sub --nsg "" --private-ip-address "$4" \
    --public-ip-sku Standard --accelerated-networking true --ppg ppg \
    --os-disk-size-gb "$3" --storage-sku Premium_LRS -o none
  az vm auto-shutdown -g "$RG" -n "$1" --time "$when" -o none
}
create srv "$SERVER_SIZE" 128 10.40.0.10 &
create cl1 "$CLIENT_SIZE" 64 10.40.0.11 &
create cl2 "$CLIENT_SIZE" 64 10.40.0.12 &
wait
az vm list-ip-addresses -g "$RG" \
  --query "[].{vm:virtualMachine.name,public:virtualMachine.network.publicIpAddresses[0].ipAddress,private:virtualMachine.network.privateIpAddresses[0]}" -o table
