#!/bin/bash
# Creates the two VMs of the memory suite ("pair A") in one proximity placement group:
#   mem-srv  Standard_E32ds_v7  (32 vCPU, 256 GiB, local NVMe)  runs every engine, one at a time
#   mem-cli  Standard_D32ls_v7  (32 vCPU, 64 GiB)                runs memtier_benchmark and the orchestration
# The sizes of the 1.2.0 memory suite's pair A. The resource group may already exist, in any region: the
# placement group, network and VMs are all created in <region> explicitly.
# Usage: create-vms.sh <resource-group> <region> <ssh-public-key-file> [operator-ip]
set -euo pipefail
RG=$1; LOC=$2; KEY=$3
MYIP=${4:-$(curl -s https://api.ipify.org)}
IMG=Canonical:ubuntu-24_04-lts:server:latest
az group show -n "$RG" -o none 2>/dev/null || az group create -n "$RG" -l "$LOC" -o none
az ppg create -g "$RG" -n mem-ppg -l "$LOC" -t Standard -o none
az network vnet create -g "$RG" -n mem-vnet -l "$LOC" --address-prefix 10.30.0.0/16 --subnet-name s --subnet-prefix 10.30.0.0/24 -o none
az network nsg create -g "$RG" -n mem-nsg -l "$LOC" -o none
az network nsg rule create -g "$RG" --nsg-name mem-nsg -n ssh-operator --priority 100 \
  --source-address-prefixes "$MYIP/32" --destination-port-ranges 22 --access Allow --protocol Tcp -o none
for spec in mem-srv:Standard_E32ds_v7:10.30.0.4 mem-cli:Standard_D32ls_v7:10.30.0.5; do
  IFS=: read -r N SIZE IP <<< "$spec"
  az vm create -g "$RG" -n "$N" -l "$LOC" --size "$SIZE" --image "$IMG" --admin-username azureuser \
    --ssh-key-values "$KEY" --vnet-name mem-vnet --subnet s --nsg mem-nsg --nsg-rule NONE --private-ip-address "$IP" \
    --public-ip-sku Standard --accelerated-networking true --ppg mem-ppg \
    --os-disk-size-gb 128 --storage-sku Premium_LRS -o table
  # backstop: shut down 8 hours from now even if teardown is forgotten
  az vm auto-shutdown -g "$RG" -n "$N" --time "$(date -u -d '+8 hours' +%H%M)" -o none || true
done
az vm list-ip-addresses -g "$RG" -o table
