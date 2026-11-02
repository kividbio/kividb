D="$1"; RG=kividb-rc4-aks; ACR=$(cat "$D/acrname")
IP=$(curl -s -m 5 https://api.ipify.org); echo "ip $IP $(date -u)"
for SKU in Standard_D4as_v7 Standard_D4ads_v7; do
  echo "try $SKU $(date -u)"
  if az aks create -g $RG -n e2e --tier free --node-count 4 --node-vm-size $SKU --network-plugin azure --network-plugin-mode overlay --attach-acr $ACR --generate-ssh-keys --api-server-authorized-ip-ranges "${IP%.*.*}.0.0/16" -o none; then echo "OK $SKU"; break; fi
done
az aks get-credentials -g $RG -n e2e --file "$D/kubeconfig" --overwrite-existing
date -u; echo PROVISION_DONE
