#!/bin/bash
# azure-create.sh -- creates the test bed: PAIRS server/client pairs of Standard_D4pls_v5
# (4 vCPU arm64 Ampere Altra / Neoverse-N1, 8 GiB) plus one builder VM of the same size,
# all in one proximity placement group and one subnet, SSH open to one address only.
#
#   RG=kividb-rc4-cachepi L=northeurope MYIP=<your public ip> PUBKEY=~/.ssh/id_ed25519.pub \
#   INTERNAL_PUBKEY=internal.pub PAIRS=7 bash azure-create.sh
#
# Private addresses: server i = 10.30.1.(10+i), client i = 10.30.1.(20+i), builder = 10.30.1.30.
# INTERNAL_PUBKEY is a throwaway key whose private half is copied to every VM, so clients can
# restart their server between runs and the builder can push binaries.
set -e
: "${RG:?}" "${L:?}" "${MYIP:?}" "${PUBKEY:?}" "${INTERNAL_PUBKEY:?}"
PAIRS=${PAIRS:-7}
SIZE=${SIZE:-Standard_D4pls_v5}
IMAGE=Canonical:ubuntu-24_04-lts:server-arm64:latest
[ "$(az group exists -n "$RG")" = true ] || az group create -n "$RG" -l "$L" -o none
az ppg create -g "$RG" -n "ppg-$L" -l "$L" --type Standard -o none
az network vnet create -g "$RG" -n "vnet-$L" -l "$L" --address-prefix 10.30.0.0/16 \
  --subnet-name sub --subnet-prefix 10.30.1.0/24 -o none
az network nsg create -g "$RG" -n "nsg-$L" -l "$L" -o none
az network nsg rule create -g "$RG" --nsg-name "nsg-$L" -n ssh --priority 100 \
  --source-address-prefixes "$MYIP/32" --destination-port-ranges 22 --access Allow --protocol Tcp -o none
az network vnet subnet update -g "$RG" --vnet-name "vnet-$L" -n sub --network-security-group "nsg-$L" -o none
mk() { # name ip
  az vm create -g "$RG" -n "$1" --image "$IMAGE" --size "$SIZE" --ppg "ppg-$L" -l "$L" \
    --vnet-name "vnet-$L" --subnet sub --private-ip-address "$2" --nsg "" \
    --public-ip-sku Standard --accelerated-networking true \
    --admin-username azureuser --ssh-key-values "$PUBKEY" "$INTERNAL_PUBKEY" \
    --os-disk-size-gb 64 --no-wait -o none
}
mk b1 10.30.1.30
for i in $(seq 1 "$PAIRS"); do
  mk "s$i" "10.30.1.$((10 + i))"
  mk "c$i" "10.30.1.$((20 + i))"
done
echo "creation requested; wait with: az vm wait -g $RG --ids \$(az vm list -g $RG --query [].id -o tsv) --created"
