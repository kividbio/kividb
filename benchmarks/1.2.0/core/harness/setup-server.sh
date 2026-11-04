#!/usr/bin/env bash
# Install every engine on the server (Ubuntu 24.04 arm64), pinned versions.
# Run as the login user, once. Binaries go to ~/bin, Garnet to ~/garnet.
#
#   bash setup-server.sh
#
# KiviDB builds under test are added separately (fetch-kividb.sh).
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive

REDIS_VERSION=${REDIS_VERSION:-8.10.3}
VALKEY_VERSION=${VALKEY_VERSION:-9.1.3}
DRAGONFLY_VERSION=${DRAGONFLY_VERSION:-v2.0.2}
GARNET_VERSION=${GARNET_VERSION:-v2.2.5}
DOTNET_CHANNEL=${DOTNET_CHANNEL:-10.0}
KIVIDB_OLD=${KIVIDB_OLD:-v1.1.3}

sudo apt-get update -qq
sudo apt-get install -y -qq build-essential pkg-config libssl-dev cmake clang git curl jq \
  sysstat python3 xz-utils libicu-dev numactl > /dev/null

# Same OS settings as infra/benchmark-azure/bootstrap/server.sh.
sudo sysctl -qw net.core.somaxconn=65535 net.ipv4.tcp_max_syn_backlog=65535
echo '* soft nofile 1000000
* hard nofile 1000000' | sudo tee /etc/security/limits.d/bench.conf > /dev/null
for gov in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do
  [ -w "$gov" ] && echo performance | sudo tee "$gov" > /dev/null || true
done

mkdir -p ~/bin ~/src
cd ~/src

# Redis, from its release tag, plain `make` (jemalloc, no modules: strings only).
if [ ! -x ~/bin/redis-server ]; then
  curl -fsSL "https://github.com/redis/redis/archive/refs/tags/${REDIS_VERSION}.tar.gz" -o redis.tgz
  tar -xzf redis.tgz
  (cd "redis-${REDIS_VERSION}" && make -j"$(nproc)" > ~/src/redis-build.log 2>&1)
  install -m 0755 "redis-${REDIS_VERSION}/src/redis-server" "redis-${REDIS_VERSION}/src/redis-cli" \
    "redis-${REDIS_VERSION}/src/redis-benchmark" ~/bin/
fi

# Valkey: the project's own binary package.
if [ ! -x ~/bin/valkey-server ]; then
  curl -fsSL "https://download.valkey.io/releases/valkey-${VALKEY_VERSION}-noble-arm64.tar.gz" -o valkey.tgz
  tar -xzf valkey.tgz
  install -m 0755 "valkey-${VALKEY_VERSION}-noble-arm64/bin/valkey-server" ~/bin/valkey-server
fi

# Dragonfly: release binary.
if [ ! -x ~/bin/dragonfly ]; then
  curl -fsSL "https://github.com/dragonflydb/dragonfly/releases/download/${DRAGONFLY_VERSION}/dragonfly-aarch64.tar.gz" -o df.tgz
  tar -xzf df.tgz
  install -m 0755 dragonfly-aarch64 ~/bin/dragonfly
fi

# Garnet: release package on the .NET runtime it targets.
if [ ! -x ~/garnet/GarnetServer ]; then
  curl -fsSL https://dot.net/v1/dotnet-install.sh -o dotnet-install.sh
  bash dotnet-install.sh --runtime dotnet --channel "$DOTNET_CHANNEL" --install-dir ~/dotnet > /dev/null
  curl -fsSL "https://github.com/microsoft/garnet/releases/download/${GARNET_VERSION}/linux-arm64-based.tar.xz" -o garnet.txz
  rm -rf g && mkdir g && tar -xJf garnet.txz -C g
  rm -rf ~/garnet && mv "g/net${DOTNET_CHANNEL}" ~/garnet
  chmod +x ~/garnet/GarnetServer
  echo "$GARNET_VERSION" > ~/garnet/VERSION
fi

# KiviDB previous release (default variant), from its GitHub release.
if [ ! -x ~/bin/kividb-1.1.3 ]; then
  curl -fsSL "https://github.com/kividbio/kividb/releases/download/${KIVIDB_OLD}/kividb-linux-aarch64.tar.gz" -o k113.tgz
  rm -rf k113 && mkdir k113 && tar -xzf k113.tgz -C k113
  install -m 0755 "$(find k113 -type f -name kividb | head -1)" ~/bin/kividb-1.1.3
fi

# Rust, for building a KiviDB tree when no release binary exists yet.
if [ ! -x ~/.cargo/bin/cargo ]; then
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain stable > /dev/null
fi

echo "redis:     $(~/bin/redis-server --version)"
echo "valkey:    $(~/bin/valkey-server --version)"
echo "dragonfly: $(~/bin/dragonfly --version 2>&1 | head -1)"
echo "garnet:    $(cat ~/garnet/VERSION) on $(~/dotnet/dotnet --list-runtimes | head -1)"
echo "kividb:    $(~/bin/kividb-1.1.3 --version 2>&1 | head -1)"
uname -r
