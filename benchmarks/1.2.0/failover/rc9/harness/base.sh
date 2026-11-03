#!/usr/bin/env bash
# base.sh <srv|client|build>: packages on one VM (Ubuntu 24.04). srv/client: redis-tools, /opt/kv, data on the local
# NVMe disk where there is one (/mnt on D8ds_v5); client also Python (redis, hiredis) and Go for soakgen;
# build: Docker (the release.yml container build), valgrind and redis-tools (callgrind).
set -u
export DEBIAN_FRONTEND=noninteractive
R=$1
sudo apt-get update -qq
P="redis-tools jq python3-venv curl iproute2 sysstat git"
[ "$R" = build ] && P="$P docker.io valgrind"
sudo apt-get install -y -qq $P >/dev/null 2>&1 || sudo apt-get install -y -qq $P
case $R in client) python3 -m venv ~/venv && ~/venv/bin/pip install -q redis hiredis
  curl -sL https://go.dev/dl/go1.24.9.linux-amd64.tar.gz | sudo tar xz -C /usr/local ;; esac
sudo mkdir -p /opt/kv; sudo chown azureuser /opt/kv
if mountpoint -q /mnt; then sudo chown azureuser /mnt; fi
sudo mkdir -p /data; sudo chown azureuser /data
echo 'azureuser soft nofile 65535' | sudo tee -a /etc/security/limits.conf >/dev/null
echo 'azureuser hard nofile 65535' | sudo tee -a /etc/security/limits.conf >/dev/null
redis-cli --version
echo BASE-OK $R
