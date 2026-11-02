#!/usr/bin/env bash
# Load generator setup (Ubuntu 24.04 arm64): memtier_benchmark from its
# release tag, redis-cli, sysstat. Run as the login user, once.
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive
MEMTIER_VERSION=${MEMTIER_VERSION:-2.5.2}

sudo apt-get update -qq
sudo apt-get install -y -qq build-essential autoconf automake libpcre3-dev libevent-dev \
  pkg-config zlib1g-dev libssl-dev git curl jq sysstat python3 redis-tools > /dev/null
sudo systemctl disable --now redis-server > /dev/null 2>&1 || true
echo '* soft nofile 1000000
* hard nofile 1000000' | sudo tee /etc/security/limits.d/bench.conf > /dev/null
sudo sysctl -qw net.ipv4.ip_local_port_range="1024 65000"

if ! memtier_benchmark --version 2>/dev/null | grep -q "$MEMTIER_VERSION"; then
  mkdir -p ~/src && cd ~/src
  rm -rf memtier_benchmark
  git clone -q --depth 1 --branch "$MEMTIER_VERSION" https://github.com/RedisLabs/memtier_benchmark
  cd memtier_benchmark
  autoreconf -ivf > /dev/null 2>&1
  ./configure > /dev/null
  make -j"$(nproc)" > /dev/null
  sudo make install > /dev/null
fi
memtier_benchmark --version | head -1
