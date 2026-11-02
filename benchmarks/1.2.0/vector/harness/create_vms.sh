#!/usr/bin/env bash
# Server + client pair for the vector benchmark, as the repo's
# infra/benchmark-azure/create.sh makes it for vector-benchmark.yml:
# Ubuntu 22.04 arm64, accelerated networking, one proximity placement group,
# server D32ps_v5 (32 vCPU, 128 GB), client D16ps_v5 (16 vCPU, 64 GB).
# Usage: RG=kividb-rc4-vector LOCATION=swedencentral SSH_PUB=key.pub SSH_CIDR=1.2.3.4/32 ./create_vms.sh
set -euo pipefail
: "${RG:?}" "${SSH_PUB:?}" "${SSH_CIDR:?}"
LOCATION="${LOCATION:-swedencentral}"
SERVER_SIZE="${SERVER_SIZE:-Standard_D32ps_v5}"
CLIENT_SIZE="${CLIENT_SIZE:-Standard_D16ps_v5}"
IMAGE="Canonical:0001-com-ubuntu-server-jammy:22_04-lts-arm64:latest"
P=vec
export AZURE_CORE_ONLY_SHOW_ERRORS=true

az group create -n "$RG" -l "$LOCATION" -o none
az network nsg create -g "$RG" -n $P-nsg -o none
az network nsg rule create -g "$RG" --nsg-name $P-nsg -n ssh --priority 100 \
  --access Allow --protocol Tcp --direction Inbound \
  --source-address-prefixes "$SSH_CIDR" --destination-port-ranges 22 -o none
az network vnet create -g "$RG" -n $P-vnet --address-prefix 10.90.0.0/16 \
  --subnet-name bench --subnet-prefix 10.90.1.0/24 --network-security-group $P-nsg -o none
az ppg create -g "$RG" -n $P-ppg -t Standard --intent-vm-sizes "$SERVER_SIZE" "$CLIENT_SIZE" -o none

shutdown_at="$(date -u -d '+8 hours' +%H%M)"
vm() {
  local role=$1 size=$2 disk=$3
  az network public-ip create -g "$RG" -n $P-$role-ip --sku Standard --allocation-method Static -o none
  az network nic create -g "$RG" -n $P-$role-nic --vnet-name $P-vnet --subnet bench \
    --public-ip-address $P-$role-ip --accelerated-networking true -o none
  az vm create -g "$RG" -n $P-$role --size "$size" --image "$IMAGE" \
    --admin-username ubuntu --ssh-key-values "$SSH_PUB" --nics $P-$role-nic \
    --os-disk-size-gb "$disk" --ppg $P-ppg -o none
  az vm auto-shutdown -g "$RG" -n $P-$role --time "$shutdown_at" -o none
}
vm server "$SERVER_SIZE" 128
vm client "$CLIENT_SIZE" 256
for r in server client; do
  echo "$r public=$(az network public-ip show -g "$RG" -n $P-$r-ip --query ipAddress -o tsv | tr -d '\r') private=$(az network nic show -g "$RG" -n $P-$r-nic --query 'ipConfigurations[0].privateIPAddress' -o tsv | tr -d '\r')"
done
