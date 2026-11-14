#!/usr/bin/env bash
# Creates the server + client pair for one benchmark run in Azure, in the
# existing resource group $RESOURCE_GROUP. Every resource is named
# "kvb-<RUN_ID>-..." so destroy.sh can find all of them by name alone, with
# no state handed over between jobs.
#
# Prints key=value lines for the caller, and appends them to $GITHUB_OUTPUT
# when that is set.
set -euo pipefail

: "${RUN_ID:?RUN_ID is required}"
: "${SSH_PUBLIC_KEY:?SSH_PUBLIC_KEY is required}"
RESOURCE_GROUP="${RESOURCE_GROUP:-kividb-benchmark}"
LOCATION="${LOCATION:-centralindia}"
# "false": only the server VM (the regression suite runs client and server on
# one machine); the client's outputs are then left out.
CREATE_CLIENT="${CREATE_CLIENT:-true}"
SERVER_SIZE="${SERVER_SIZE:-Standard_D32ps_v5}"
CLIENT_SIZE="${CLIENT_SIZE:-Standard_D32ps_v5}"
IMAGE="${IMAGE:-Canonical:0001-com-ubuntu-server-jammy:22_04-lts-arm64:latest}"
SSH_CIDR="${SSH_CIDR:-0.0.0.0/0}"
USE_PROXIMITY_GROUP="${USE_PROXIMITY_GROUP:-false}"
# "true": the server warms Dragonfly/Redis and the client builds memtier and
# Rust. "false": bare Ubuntu, for workflows that ship everything themselves.
BOOTSTRAP="${BOOTSTRAP:-true}"
DRAGONFLY_VERSION="${DRAGONFLY_VERSION:-v2.0.2}"
# Set REGISTER_RUNNER=true with GITHUB_REPOSITORY and RUNNER_TOKEN to have the
# client register itself as an ephemeral self-hosted runner.
REGISTER_RUNNER="${REGISTER_RUNNER:-false}"
GITHUB_REPOSITORY="${GITHUB_REPOSITORY:-}"
RUNNER_TOKEN="${RUNNER_TOKEN:-}"
RUNNER_LABELS="${RUNNER_LABELS:-self-hosted,benchmark,client}"
RUNNER_VERSION="${RUNNER_VERSION:-latest}"
# Safety net if teardown never runs: each VM stops itself after this long.
AUTO_SHUTDOWN_HOURS="${AUTO_SHUTDOWN_HOURS:-5}"

export AZURE_CORE_ONLY_SHOW_ERRORS=true

# The Azure CLI on GitHub's runners sometimes dies on its own import race
# ("deadlock detected by _ModuleLock"). Every call here is safe to repeat.
az() {
  local i
  for i in 1 2 3; do
    command az "$@" && return 0
    echo "az $1 $2 failed (attempt $i of 3)" >&2
    sleep 10
  done
  return 1
}

HERE="$(cd "$(dirname "$0")" && pwd)"
PREFIX="kvb-$RUN_ID"
TAGS=(Project=kividb-benchmark "RunId=$RUN_ID")
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

out() {
  echo "$1=$2"
  if [[ -n "${GITHUB_OUTPUT:-}" ]]; then echo "$1=$2" >> "$GITHUB_OUTPUT"; fi
}

printf '%s\n' "$SSH_PUBLIC_KEY" > "$WORK/key.pub"

echo "Network: $PREFIX-vnet in $LOCATION"
az network nsg create -g "$RESOURCE_GROUP" -l "$LOCATION" -n "$PREFIX-nsg" \
  --tags "${TAGS[@]}" -o none
# Traffic inside the virtual network (Redis ports, ping) is allowed by the
# NSG's default rules; only SSH from outside needs a rule.
az network nsg rule create -g "$RESOURCE_GROUP" --nsg-name "$PREFIX-nsg" -n ssh \
  --priority 100 --access Allow --protocol Tcp --direction Inbound \
  --source-address-prefixes "$SSH_CIDR" --destination-port-ranges 22 -o none
az network vnet create -g "$RESOURCE_GROUP" -l "$LOCATION" -n "$PREFIX-vnet" \
  --address-prefix 10.90.0.0/16 --subnet-name bench --subnet-prefix 10.90.1.0/24 \
  --network-security-group "$PREFIX-nsg" --tags "${TAGS[@]}" -o none

PPG_ARGS=()
if [[ "$USE_PROXIMITY_GROUP" == "true" ]]; then
  # The intended sizes let Azure pick a datacenter that has both. Each
  # once: Azure refuses a list that names one size twice.
  SIZES=("$SERVER_SIZE")
  if [[ "$CLIENT_SIZE" != "$SERVER_SIZE" ]]; then SIZES+=("$CLIENT_SIZE"); fi
  az ppg create -g "$RESOURCE_GROUP" -l "$LOCATION" -n "$PREFIX-ppg" -t Standard \
    --intent-vm-sizes "${SIZES[@]}" --tags "${TAGS[@]}" -o none
  PPG_ARGS=(--ppg "$PREFIX-ppg")
fi

# Values the bootstrap scripts read, ahead of the script body. %q quotes them
# for bash, so a value can never become code.
render() {
  local role="$1" dest="$2"
  {
    echo '#!/bin/bash'
    printf 'DRAGONFLY_VERSION=%q\n' "$DRAGONFLY_VERSION"
    printf 'REGISTER_RUNNER=%q\n' "$REGISTER_RUNNER"
    printf 'GITHUB_REPOSITORY=%q\n' "$GITHUB_REPOSITORY"
    printf 'RUNNER_TOKEN=%q\n' "$RUNNER_TOKEN"
    printf 'RUNNER_NAME=%q\n' "$PREFIX-client"
    printf 'RUNNER_LABELS=%q\n' "$RUNNER_LABELS"
    printf 'RUNNER_VERSION=%q\n' "$RUNNER_VERSION"
    tail -n +2 "$HERE/bootstrap/$role.sh"
  } > "$dest"
}

shutdown_at="$(date -u -d "+${AUTO_SHUTDOWN_HOURS} hours" +%H%M)"

create_vm() {
  local role="$1" size="$2" disk_gb="$3"
  local name="$PREFIX-$role"
  echo "VM: $name ($size)"
  az network public-ip create -g "$RESOURCE_GROUP" -l "$LOCATION" -n "$name-ip" \
    --sku Standard --allocation-method Static --tags "${TAGS[@]}" -o none
  az network nic create -g "$RESOURCE_GROUP" -l "$LOCATION" -n "$name-nic" \
    --vnet-name "$PREFIX-vnet" --subnet bench --public-ip-address "$name-ip" \
    --accelerated-networking true --tags "${TAGS[@]}" -o none
  local data_args=()
  if [[ "$BOOTSTRAP" == "true" ]]; then
    render "$role" "$WORK/$role.sh"
    data_args=(--custom-data "$WORK/$role.sh")
  fi
  az vm create -g "$RESOURCE_GROUP" -l "$LOCATION" -n "$name" \
    --size "$size" --image "$IMAGE" \
    --admin-username ubuntu --ssh-key-values "$WORK/key.pub" \
    --nics "$name-nic" --nic-delete-option Delete \
    --os-disk-name "$name-os" --os-disk-size-gb "$disk_gb" --os-disk-delete-option Delete \
    "${PPG_ARGS[@]}" "${data_args[@]}" --tags "${TAGS[@]}" -o none
  az vm auto-shutdown -g "$RESOURCE_GROUP" -n "$name" --time "$shutdown_at" -o none
}

# Server first, so a proximity group is anchored on the machine that matters.
create_vm server "$SERVER_SIZE" 100
if [[ "$CREATE_CLIENT" == "true" ]]; then
  create_vm client "$CLIENT_SIZE" 64
fi

# tr: the Windows CLI ends lines with \r.
ip() { az network public-ip show -g "$RESOURCE_GROUP" -n "$1" --query ipAddress -o tsv | tr -d '\r'; }
private_ip() { az network nic show -g "$RESOURCE_GROUP" -n "$1" --query 'ipConfigurations[0].privateIPAddress' -o tsv | tr -d '\r'; }

out run_id "$RUN_ID"
out resource_group "$RESOURCE_GROUP"
out server_name "$PREFIX-server"
[[ "$CREATE_CLIENT" == "true" ]] && out client_name "$PREFIX-client"
out server_private_ip "$(private_ip "$PREFIX-server-nic")"
out server_public_ip "$(ip "$PREFIX-server-ip")"
[[ "$CREATE_CLIENT" == "true" ]] && out client_private_ip "$(private_ip "$PREFIX-client-nic")"
[[ "$CREATE_CLIENT" == "true" ]] && out client_public_ip "$(ip "$PREFIX-client-ip")"
out ssh_user ubuntu
