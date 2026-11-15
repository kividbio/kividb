#!/usr/bin/env bash
# setup.sh <role>: cc = client (venv, redis-tools, jq, an ssh key for the nodes); n = node (redis-tools, /opt/kv, /data).
# The 35a413c0 release binary is copied to /opt/kv/kividb from the laptop (built on fp per release.yml).
set -u
export DEBIAN_FRONTEND=noninteractive
exec > "$HOME/setup.log" 2>&1
sudo apt-get update -qq
sudo apt-get install -y -qq redis-tools jq python3-venv curl iproute2 >/dev/null 2>&1
echo '* soft nofile 65535' | sudo tee -a /etc/security/limits.conf >/dev/null
echo '* hard nofile 65535' | sudo tee -a /etc/security/limits.conf >/dev/null
sudo mkdir -p /opt/kv /opt/kv120 /data; sudo chown azureuser /opt/kv /opt/kv120 /data
d=$(mktemp -d); cd $d
curl -fsSLO https://github.com/kividbio/kividb/releases/download/v1.2.0/kividb-linux-x86_64.tar.gz
curl -fsSLO https://github.com/kividbio/kividb/releases/download/v1.2.0/SHA256SUMS.txt
sha256sum -c --ignore-missing SHA256SUMS.txt > ~/sha-1.2.0.txt
tar xzf kividb-linux-x86_64.tar.gz; install -m 755 kividb/kividb /opt/kv120/kividb; cd ~
if [ "$1" = cc ]; then
  python3 -m venv ~/venv && ~/venv/bin/pip -q install redis==5.2.1
  ssh-keygen -q -t ed25519 -N "" -f ~/.ssh/id_ed25519 <<< y >/dev/null 2>&1
fi
echo SETUP-OK $1
