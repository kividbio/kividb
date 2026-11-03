#!/usr/bin/env bash
# rc10 core check: srv (D32ps_v5), cl1/cl2 (D16ps_v5) in one PPG.
set -euo pipefail
RG=${RG:-kividb-rc10-core}
LOCATION=${LOCATION:-centralindia}
KEY=${KEY:?ssh public key file}
SSH_CIDR=${SSH_CIDR:?cidr}
az group create -n "$RG" -l "$LOCATION" -o none
az ppg create -n ppg -g "$RG" -l "$LOCATION" -t standard \
  --intent-vm-sizes Standard_D32ps_v5 Standard_D16ps_v5 -o none
az network nsg create -g "$RG" -n nsg -l "$LOCATION" -o none
az network nsg rule create -g "$RG" --nsg-name nsg -n ssh --priority 100 \
  --source-address-prefixes "$SSH_CIDR" --destination-port-ranges 22 \
  --access Allow --protocol Tcp -o none
az network vnet create -g "$RG" -n vnet -l "$LOCATION" --address-prefix 10.40.0.0/16 \
  --subnet-name sub --subnet-prefix 10.40.0.0/24 -o none
az network vnet subnet update -g "$RG" --vnet-name vnet -n sub --network-security-group nsg -o none
when=$(date -u -d "+4 hours" +%H%M)
create() { # name size disk ip image ppg
  local extra=()
  [ "$6" = yes ] && extra=(--ppg ppg)
  az vm create -g "$RG" -n "$1" -l "$LOCATION" --size "$2" --image "$5" \
    --admin-username azureuser --ssh-key-values "$KEY" \
    --vnet-name vnet --subnet sub --nsg "" --private-ip-address "$4" \
    --public-ip-sku Standard --accelerated-networking true "${extra[@]}" \
    --os-disk-size-gb "$3" --storage-sku Premium_LRS -o none
  az vm auto-shutdown -g "$RG" -n "$1" --time "$when" -o none
  echo "created $1"
}
ARM=Canonical:ubuntu-24_04-lts:server-arm64:latest
X86=Canonical:ubuntu-24_04-lts:server:latest
create srv Standard_D32ps_v5 128 10.40.0.10 $ARM yes &
create cl1 Standard_D16ps_v5 64 10.40.0.11 $ARM yes &
create cl2 Standard_D16ps_v5 64 10.40.0.12 $ARM yes &
wait
az vm list-ip-addresses -g "$RG" \
  --query "[].{vm:virtualMachine.name,public:virtualMachine.network.publicIpAddresses[0].ipAddress,private:virtualMachine.network.privateIpAddresses[0]}" -o tsv
