#!/usr/bin/env bash
# create.sh <ssh-public-key-file>: RG kividb-rc9-retest. Auto-shutdown 6 h out on every VM, SSH from this IP only.
#   westus2 (vnet 10.91.0.0/16):  b1 D16s_v5 (builds, callgrind); set A t1,t2 D8ds_v5 + tc D8s_v5 (items 1-5);
#                                 set B sm,sr D8ds_v5 + sc D8s_v5 (item 6 soak)
#   canadacentral (10.40.0.0/16): srv D32ps_v5 + cl1,cl2 D16ps_v5 (arm64, the rc8 core harness shapes)
set -u
RG=kividb-rc9-retest K=$1
IP=$(curl -s https://api.ipify.org)
SD=$(date -u -d '+6 hours' +%H%M)
az group create -n $RG -l westus2 --tags purpose=rc9-retest -o none
net() { # region prefix
  az network vnet create -g $RG -l $1 -n v-$1 --address-prefix $2.0.0/16 --subnet-name s1 --subnet-prefix $2.$3.0/24 -o none
  az network nsg create -g $RG -l $1 -n n-$1 -o none
  az network nsg rule create -g $RG --nsg-name n-$1 -n ssh --priority 100 --source-address-prefixes "$IP/32" --destination-port-ranges 22 --access Allow --protocol Tcp -o none
  az network vnet subnet update -g $RG --vnet-name v-$1 -n s1 --network-security-group n-$1 -o none
}
net westus2 10.91 1 & net canadacentral 10.40 0 & wait
az ppg create -g $RG -n ppg-a -l westus2 -o none & az ppg create -g $RG -n ppg-b -l westus2 -o none &
az ppg create -g $RG -n ppg-c -l canadacentral -t standard --intent-vm-sizes Standard_D32ps_v5 Standard_D16ps_v5 -o none & wait
mk() { # name size region ip image ppg disk
  local ppg=(); [ -n "$6" ] && ppg=(--ppg "$6")
  out=$(az vm create -g $RG -l $3 -n $1 --size $2 --image $5 --admin-username azureuser --ssh-key-values @$K \
    --vnet-name v-$3 --subnet s1 --nsg "" --private-ip-address $4 --public-ip-sku Standard --os-disk-size-gb $7 \
    --storage-sku Premium_LRS --accelerated-networking true "${ppg[@]}" -o tsv --query publicIpAddress --only-show-errors 2>&1)
  if echo "$out" | grep -q -E 'ERROR|Error'; then echo "FAIL $1 $2 $3: $(echo "$out" | grep -v '^$' | tail -2 | tr '\n' ' ')"; return; fi
  az vm auto-shutdown -g $RG -n $1 --time "$SD" -o none && echo "made $1 (auto-shutdown $SD UTC)"
}
X86=Canonical:ubuntu-24_04-lts:server:latest ARM=Canonical:ubuntu-24_04-lts:server-arm64:latest
mk b1 Standard_D16s_v5 westus2 10.91.1.10 $X86 "" 128 &
mk t1 Standard_D8ds_v5 westus2 10.91.1.11 $X86 ppg-a 128 &
mk t2 Standard_D8ds_v5 westus2 10.91.1.12 $X86 ppg-a 128 &
mk tc Standard_D8s_v5  westus2 10.91.1.13 $X86 ppg-a 128 &
mk sm Standard_D8ds_v5 westus2 10.91.1.21 $X86 ppg-b 128 &
mk sr Standard_D8ds_v5 westus2 10.91.1.22 $X86 ppg-b 128 &
mk sc Standard_D8s_v5  westus2 10.91.1.23 $X86 ppg-b 128 &
mk srv Standard_D32ps_v5 canadacentral 10.40.0.10 $ARM ppg-c 128 &
mk cl1 Standard_D16ps_v5 canadacentral 10.40.0.11 $ARM ppg-c 64 &
mk cl2 Standard_D16ps_v5 canadacentral 10.40.0.12 $ARM ppg-c 64 &
wait
az vm list -g $RG -d --query "[].[name,publicIps,privateIps,location]" -o tsv
