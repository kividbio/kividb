#!/usr/bin/env bash
# Create RG kividb-rc7-failover (VMs in koreacentral: germanywestcentral and francecentral had no capacity), a vnet, NSG (SSH from the laptop's /16 only), and the VMs.
# Auto-shutdown on every VM.
set -u
RG=kividb-rc7-failover L=${L:-germanywestcentral}
K="${SSH_PUBKEY:-$HOME/.ssh/id_ed25519.pub}"
IP=$(curl -s https://api.ipify.org)
NET=$(echo "$IP" | awk -F. '{print $1"."$2".0.0/16"}')
az group exists -n $RG | grep -q true || az group create -n $RG -l $L --tags purpose=rc7-failover -o none
az network vnet create -g $RG -l $L -n fo-vnet --address-prefix 10.60.0.0/16 --subnet-name s1 --subnet-prefix 10.60.1.0/24 -o none
az network nsg create -g $RG -l $L -n fo-nsg -o none
az network nsg rule create -g $RG --nsg-name fo-nsg -n ssh --priority 100 --source-address-prefixes "$NET" --destination-port-ranges 22 --access Allow --protocol Tcp -o none
az network vnet subnet update -g $RG --vnet-name fo-vnet -n s1 --network-security-group fo-nsg -o none
az ppg create -g $RG -n fo-ppg -l $L -o none
az ppg create -g $RG -n ra-ppg -l $L -o none
SD=$(date -u -d '+10 hours' +%H%M)
mk() { # name size [extra]
  az vm create -g $RG -n $1 --size $2 -l $L --image Canonical:ubuntu-24_04-lts:server:latest --admin-username azureuser \
    --ssh-key-values @"$K" --vnet-name fo-vnet --subnet s1 --nsg "" --public-ip-sku Standard --os-disk-size-gb ${OSD:-64} \
    --storage-sku Premium_LRS --accelerated-networking true ${3:-} -o none --only-show-errors 2>&1 | grep -E "^ *Message" | tail -1
  az vm auto-shutdown -g $RG -n $1 --time "$SD" -o none
  echo "made $1"
}
for v in fo1 fo2 fo3 foc; do mk $v Standard_D4s_v5 "--ppg fo-ppg" & done
mk pf1 Standard_D4s_v5 "--ppg fo-ppg" &
mk pfc Standard_D4s_v5 "--ppg fo-ppg" &
OSD=128 mk bld Standard_D16s_v5 &
mk sm Standard_D8ds_v5 "--ppg ra-ppg" &
mk sr Standard_D8ds_v5 "--ppg ra-ppg" &
mk sc Standard_D8s_v5 "--ppg ra-ppg" &
wait
az vm list -g $RG -d --query "[].[name,publicIps,privateIps]" -o tsv
