#!/usr/bin/env bash
# base setup on every VM: role in $1 (srv | client | ab)
set -u
export DEBIAN_FRONTEND=noninteractive
R=$1
sudo apt-get update -qq
P="redis-tools jq python3-venv curl iproute2 sysstat"
sudo apt-get install -y -qq $P >/dev/null 2>&1 || sudo apt-get install -y -qq $P
curl -sL https://github.com/oras-project/oras/releases/download/v1.2.2/oras_1.2.2_linux_amd64.tar.gz | sudo tar xz -C /usr/local/bin oras
case $R in client|ab) python3 -m venv ~/venv && ~/venv/bin/pip install -q redis hiredis matplotlib ;; esac
case $R in client) curl -sL https://go.dev/dl/go1.24.9.linux-amd64.tar.gz | sudo tar xz -C /usr/local ;; esac
sudo mkdir -p /opt/kv; sudo chown azureuser /opt/kv
# data on the local NVMe disk where there is one (D8ds_v5: /mnt)
if mountpoint -q /mnt; then sudo chown azureuser /mnt; sudo rm -rf /data; mkdir -p /mnt/data; sudo ln -sfn /mnt/data /data; else sudo mkdir -p /data; sudo chown azureuser /data; fi
echo 'azureuser soft nofile 65535' | sudo tee -a /etc/security/limits.conf >/dev/null
echo 'azureuser hard nofile 65535' | sudo tee -a /etc/security/limits.conf >/dev/null
redis-cli --version
echo BASE-OK $R $(df -h /data/ | tail -1)
