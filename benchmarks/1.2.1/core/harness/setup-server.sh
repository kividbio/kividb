#!/usr/bin/env bash
# Server setup for the 1.2.1 check: the 1.2.0 suite's OS settings (setup-server-min.sh of
# its last round), redis-cli for engine.sh, and Docker for the release-way builds
# (release.yml builds the arm64 binaries inside amazonlinux:2023).
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive
sudo apt-get update -qq
sudo apt-get install -y -qq build-essential pkg-config libssl-dev cmake clang git curl jq \
  sysstat python3 xz-utils numactl redis-tools docker.io > /dev/null
sudo systemctl disable --now redis-server > /dev/null 2>&1 || true
sudo sysctl -qw net.core.somaxconn=65535 net.ipv4.tcp_max_syn_backlog=65535
echo '* soft nofile 1000000
* hard nofile 1000000' | sudo tee /etc/security/limits.d/bench.conf > /dev/null
for gov in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do
  [ -w "$gov" ] && echo performance | sudo tee "$gov" > /dev/null || true
done
mkdir -p ~/bin ~/src
ln -sf "$(command -v redis-cli)" ~/bin/redis-cli
sudo systemctl enable --now docker > /dev/null 2>&1 || true
sudo usermod -aG docker "$USER"
uname -r
echo SETUP-DONE
