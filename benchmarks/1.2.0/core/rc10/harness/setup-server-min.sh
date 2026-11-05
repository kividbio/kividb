#!/usr/bin/env bash
# Server setup for the rc10 check: setup-server.sh's OS settings and Rust,
# plus redis-cli (engine.sh pings with it). No competitor engines.
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive
sudo apt-get update -qq
sudo apt-get install -y -qq build-essential pkg-config libssl-dev cmake clang git curl jq \
  sysstat python3 xz-utils numactl redis-tools > /dev/null
sudo systemctl disable --now redis-server > /dev/null 2>&1 || true
sudo sysctl -qw net.core.somaxconn=65535 net.ipv4.tcp_max_syn_backlog=65535
echo '* soft nofile 1000000
* hard nofile 1000000' | sudo tee /etc/security/limits.d/bench.conf > /dev/null
for gov in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do
  [ -w "$gov" ] && echo performance | sudo tee "$gov" > /dev/null || true
done
mkdir -p ~/bin ~/src
ln -sf "$(command -v redis-cli)" ~/bin/redis-cli
if [ ! -x ~/.cargo/bin/cargo ]; then
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain stable > /dev/null
fi
~/.cargo/bin/rustc --version
uname -r
