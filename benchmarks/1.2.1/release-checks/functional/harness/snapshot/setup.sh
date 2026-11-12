#!/usr/bin/env bash
# setup.sh (pcli): redis-tools, venv, the 1.2.0 release binary (SHA256SUMS checked) at /opt/kv120/kividb.
set -u
export DEBIAN_FRONTEND=noninteractive
sudo apt-get update -qq && sudo apt-get install -y -qq redis-tools python3-venv jq >/dev/null 2>&1
python3 -m venv ~/venv && ~/venv/bin/pip install -q redis==5.2.1
sudo mkdir -p /opt/kv120 && sudo chown azureuser /opt/kv120
d=$(mktemp -d); cd $d
curl -fsSLO https://github.com/kividbio/kividb/releases/download/v1.2.0/kividb-linux-x86_64.tar.gz
curl -fsSLO https://github.com/kividbio/kividb/releases/download/v1.2.0/SHA256SUMS.txt
sha256sum -c --ignore-missing SHA256SUMS.txt | tee ~/sha-1.2.0.txt
tar xzf kividb-linux-x86_64.tar.gz; install -m 755 kividb/kividb /opt/kv120/kividb; /opt/kv120/kividb --version; sha256sum /opt/kv120/kividb
echo SETUP-OK
