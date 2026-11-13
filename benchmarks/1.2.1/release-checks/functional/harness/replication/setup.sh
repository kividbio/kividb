#!/usr/bin/env bash
# setup.sh <role>: cl = client + sentinel (no build: the release binary is copied in); n = server
set -u
export DEBIAN_FRONTEND=noninteractive
R=$1
exec > "$HOME/setup.log" 2>&1
sudo apt-get update -qq
P="redis-tools jq python3-venv curl iproute2"
[ $R = cl ] && P="$P docker.io"
sudo apt-get install -y -qq $P >/dev/null 2>&1 || sudo apt-get install -y -qq $P
echo '* soft nofile 65535' | sudo tee -a /etc/security/limits.conf >/dev/null
echo '* hard nofile 65535' | sudo tee -a /etc/security/limits.conf >/dev/null
sudo mkdir -p /opt/kv /opt/kv120 /opt/redis /data; sudo chown azureuser /opt/kv /opt/kv120 /opt/redis /data
# 1.2.0 released binary
d=$(mktemp -d); cd $d
curl -fsSLO https://github.com/kividbio/kividb/releases/download/v1.2.0/kividb-linux-x86_64.tar.gz
curl -fsSLO https://github.com/kividbio/kividb/releases/download/v1.2.0/SHA256SUMS.txt
sha256sum -c --ignore-missing SHA256SUMS.txt > ~/sha-1.2.0.txt
tar xzf kividb-linux-x86_64.tar.gz; install -m 755 kividb/kividb /opt/kv120/kividb; /opt/kv120/kividb --version
cd ~
if [ $R = cl ]; then
  python3 -m venv ~/venv && ~/venv/bin/pip install -q redis==5.2.1 hiredis==3.0.0
  sudo docker pull -q redis:8.10.3 >/dev/null
  cid=$(sudo docker create redis:8.10.3); sudo docker cp $cid:/usr/local/bin/redis-server /opt/redis/redis-server
  sudo docker cp $cid:/usr/local/bin/redis-cli /opt/redis/redis-cli; sudo docker rm $cid >/dev/null
  sudo chown azureuser /opt/redis/*; /opt/redis/redis-server --version
  # 35a413c0: the release.yml build is copied to /opt/kv/kividb from the laptop (built on fp, centralindia)
  ssh-keygen -q -t ed25519 -N "" -f ~/.ssh/id_ed25519 <<< y >/dev/null 2>&1
fi
echo SETUP-OK $R
