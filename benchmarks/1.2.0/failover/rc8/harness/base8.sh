#!/usr/bin/env bash
# base8.sh <role>: set-up on every VM for the rc8 run. roles: fo (server), foc (client), pf (power-off server),
# pfc (power-off client), bld (build + single-host probes).
# rc8 is not tagged, so there is no published binary: bld builds 082dab3d and copies kividb into /opt/kv on every VM
# (see build8.sh). This script installs the tools and unpacks the harness.
set -u
export DEBIAN_FRONTEND=noninteractive
R=$1
exec > "$HOME/base.log" 2>&1
sudo apt-get update -qq
P="redis-tools jq python3-venv curl iproute2 gdb sysstat"
case $R in bld) P="$P build-essential cmake clang pkg-config git libssl-dev" ;; esac
sudo apt-get install -y -qq $P >/dev/null 2>&1 || sudo apt-get install -y -qq $P
case $R in foc|pfc|bld) python3 -m venv ~/venv && ~/venv/bin/pip install -q redis==5.2.1 hiredis==3.0.0 matplotlib ;; esac
sudo mkdir -p /opt/kv /data; sudo chown azureuser /opt/kv /data
echo '* soft nofile 65535' | sudo tee -a /etc/security/limits.conf >/dev/null
echo '* hard nofile 65535' | sudo tee -a /etc/security/limits.conf >/dev/null
mkdir -p ~/b && tar xzf ~/b.tgz -C ~ && ln -sfn ~/b/failover ~/fo
case $R in fo|pf) cp ~/fo/harness/node.sh ~/node.sh; cp ~/fo/harness/aoftail.sh ~/aoftail.sh ;; esac
echo BASE-OK $R
