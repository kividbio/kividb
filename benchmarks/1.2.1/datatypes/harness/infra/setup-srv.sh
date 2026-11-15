#!/usr/bin/env bash
# setup-srv.sh: the server VM (Ubuntu 24.04, x86_64) for the 1.2.1 round. The 1.2.0 suite's
# setup-srv.sh minus the competitors (no Redis build, no images, no perf): the same sysctls and
# limits, Docker installed and running (as in every 1.2.0 round, so netfilter conntrack is active;
# it also runs the release build in amazonlinux:2023), redis-tools, python3-matplotlib.
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive
sudo apt-get update -qq
sudo apt-get install -y -qq build-essential git curl jq python3 python3-matplotlib docker.io redis-tools sysstat > /dev/null
sudo usermod -aG docker "$USER"
sudo systemctl start docker
echo "* soft nofile 65535" | sudo tee -a /etc/security/limits.conf > /dev/null
echo "* hard nofile 65535" | sudo tee -a /etc/security/limits.conf > /dev/null
echo 'vm.overcommit_memory = 1' | sudo tee /etc/sysctl.d/99-bench.conf > /dev/null
echo 'net.core.somaxconn = 65535' | sudo tee -a /etc/sysctl.d/99-bench.conf > /dev/null
sudo sysctl -q --system
mkdir -p "$HOME/bin" "$HOME/out"
nproc; free -g | sed -n 2p; uname -r; lscpu | grep 'Model name'
echo SETUP-OK
