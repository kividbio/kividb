#!/usr/bin/env bash
# setup-cli.sh: the client VM. memtier_benchmark 2.5.2 built from source,
# redis-tools (redis-cli), sysstat (mpstat for client CPU).
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive
MEMTIER_VER=${MEMTIER_VER:-2.5.2}
sudo apt-get update -qq
sudo apt-get install -y -qq build-essential autoconf automake libpcre3-dev libevent-dev pkg-config zlib1g-dev \
  libssl-dev git redis-tools sysstat python3 jq > /dev/null
echo "* soft nofile 65535" | sudo tee -a /etc/security/limits.conf > /dev/null
echo "* hard nofile 65535" | sudo tee -a /etc/security/limits.conf > /dev/null
if ! command -v memtier_benchmark > /dev/null; then
  (cd /tmp && rm -rf memtier && git clone -q --depth 1 --branch "$MEMTIER_VER" https://github.com/RedisLabs/memtier_benchmark memtier \
    && cd memtier && autoreconf -ivf > /dev/null 2>&1 && ./configure > /dev/null && make -j16 > /dev/null && sudo make install > /dev/null)
fi
memtier_benchmark --version | head -1
mkdir -p "$HOME/out"
nproc; free -g | sed -n 2p; uname -r; lscpu | grep 'Model name'
echo SETUP-OK
