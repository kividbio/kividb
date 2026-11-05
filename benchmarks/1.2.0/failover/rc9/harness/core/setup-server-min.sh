#!/usr/bin/env bash
# rc9 retest: the KiviDB-only part of setup-server.sh (Ubuntu 24.04 arm64): packages, the same OS settings, Rust
# stable; redis-cli for engine.sh. The competitors are not installed (the retest runs KiviDB rc9 against rc8 only).
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive
sudo apt-get update -qq
sudo apt-get install -y -qq build-essential pkg-config libssl-dev cmake clang git curl jq sysstat python3 redis-tools > /dev/null
sudo systemctl disable --now redis-server > /dev/null 2>&1 || true
sudo sysctl -qw net.core.somaxconn=65535 net.ipv4.tcp_max_syn_backlog=65535
echo '* soft nofile 1000000
* hard nofile 1000000' | sudo tee /etc/security/limits.d/bench.conf > /dev/null
for gov in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do
  [ -w "$gov" ] && echo performance | sudo tee "$gov" > /dev/null || true
done
mkdir -p ~/bin ~/src ~/h; ln -sf /usr/bin/redis-cli ~/bin/redis-cli
[ -x ~/.cargo/bin/cargo ] || curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain stable > /dev/null
~/.cargo/bin/rustc --version
echo SRV-SETUP-OK
