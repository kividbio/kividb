#!/bin/bash
# Second engine/client pair for the 20-minute mixed runs, so they run beside the other tests instead of after
# them (the first region's 65-vCPU quota was used by the first pair). Intel v6, the closest size offered in the second region (polandcentral); all five mixed runs use this pair.
#   mem-srv2  Standard_E32ds_v6  (32 vCPU, 256 GiB)   engines
#   mem-cli2  Standard_D16ls_v6  (16 vCPU)            memtier at a fixed 150k ops/s
# Usage: create-vms-b.sh <resource-group> <region> <ssh-public-key-file>
set -euo pipefail
RG=$1; LOC=$2; KEY=$3
MYIP=$(curl -s https://api.ipify.org)
IMG=Canonical:ubuntu-24_04-lts:server:latest
az ppg show -g "$RG" -n mem-ppg2 -o none 2>/dev/null || az ppg create -g "$RG" -n mem-ppg2 -l "$LOC" -t Standard -o none
az network vnet show -g "$RG" -n mem-vnet2 -o none 2>/dev/null || az network vnet create -g "$RG" -n mem-vnet2 -l "$LOC" --address-prefix 10.31.0.0/16 --subnet-name s --subnet-prefix 10.31.0.0/24 -o none
az network nsg show -g "$RG" -n mem-nsg2 -o none 2>/dev/null || az network nsg create -g "$RG" -n mem-nsg2 -l "$LOC" -o none
az network nsg rule show -g "$RG" --nsg-name mem-nsg2 -n ssh-operator -o none 2>/dev/null || az network nsg rule create -g "$RG" --nsg-name mem-nsg2 -n ssh-operator --priority 100 \
  --source-address-prefixes "$MYIP/32" --destination-port-ranges 22 --access Allow --protocol Tcp -o none
for spec in mem-srv2:Standard_E32ds_v6:10.31.0.4 mem-cli2:Standard_D16ls_v6:10.31.0.5; do
  IFS=: read -r N SIZE IP <<< "$spec"
  az vm create -g "$RG" -n "$N" -l "$LOC" --size "$SIZE" --image "$IMG" --admin-username azureuser \
    --ssh-key-values "$KEY" --vnet-name mem-vnet2 --subnet s --nsg mem-nsg2 --private-ip-address "$IP" \
    --public-ip-sku Standard --accelerated-networking true --ppg mem-ppg2 \
    --os-disk-size-gb 128 --storage-sku Premium_LRS -o table
  # backstop; not offered in every region (polandcentral has none), the RG is deleted at the end anyway
  az vm auto-shutdown -g "$RG" -n "$N" -l "$LOC" --time "$(date -u -d "+8 hours" +%H%M)" -o none || true
done
