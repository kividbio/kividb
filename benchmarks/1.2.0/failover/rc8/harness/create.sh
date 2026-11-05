#!/usr/bin/env bash
# create.sh: RG kividb-rc8-failover in japaneast (fallback canadacentral): vnet, NSG (SSH from the operator's /16 only),
# two PPGs' worth of D4s_v5 VMs for the failover scenarios (set A: fo1-fo3 + foc, set B: fo4-fo6 + fod),
# pf1 + pfc for the power-offs, and bld (D16s_v5) for the build and the single-host probes. Auto-shutdown on every VM.
set -u
RG=kividb-rc8-failover L=${L:-japaneast}
K="${SSH_PUBKEY:-$HOME/.ssh/id_ed25519.pub}"
IP=$(curl -s https://api.ipify.org)
NET=$(echo "$IP" | awk -F. '{print $1"."$2".0.0/16"}')
az group exists -n $RG | grep -q true || az group create -n $RG -l $L --tags purpose=rc8-failover -o none
az network vnet create -g $RG -l $L -n fo-vnet --address-prefix 10.61.0.0/16 --subnet-name s1 --subnet-prefix 10.61.1.0/24 -o none
az network nsg create -g $RG -l $L -n fo-nsg -o none
az network nsg rule create -g $RG --nsg-name fo-nsg -n ssh --priority 100 --source-address-prefixes "$NET" --destination-port-ranges 22 --access Allow --protocol Tcp -o none
az network vnet subnet update -g $RG --vnet-name fo-vnet -n s1 --network-security-group fo-nsg -o none
az ppg create -g $RG -n fo-ppg -l $L -o none
SD=$(date -u -d '+5 hours' +%H%M)
mk() { # name size [extra]
  az vm create -g $RG -n $1 --size $2 -l $L --image Canonical:ubuntu-24_04-lts:server:latest --admin-username azureuser \
    --ssh-key-values @"$K" --vnet-name fo-vnet --subnet s1 --nsg "" --public-ip-sku Standard --os-disk-size-gb ${OSD:-64} \
    --storage-sku Premium_LRS --accelerated-networking true ${3:-} -o none --only-show-errors 2>&1 | grep -E "Message|ERROR" | tail -1
  az vm auto-shutdown -g $RG -n $1 --time "$SD" --location $L -o none && echo "made $1 (auto-shutdown $SD UTC)"
}
OSD=128 mk bld Standard_D16s_v5 &
for v in fo1 fo2 fo3 foc fo4 fo5 fo6 fod pf1 pfc; do mk $v Standard_D4s_v5 "--ppg fo-ppg" & done
wait
az vm list -g $RG -d --query "[].[name,publicIps,privateIps]" -o tsv | tr -d '\r'
