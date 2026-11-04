#!/usr/bin/env bash
# setup-srv.sh: the server VM (Ubuntu 24.04, x86_64). Tools, perf, Docker,
# Rust, Redis 8.10.3 built from source, the competitor images, KiviDB 1.1.3
# from its GitHub release, the kividb repo, Brendan Gregg's FlameGraph.
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive
REDIS_VER=${REDIS_VER:-8.10.3}
VALKEY_IMG=${VALKEY_IMG:-valkey/valkey:9.1.3}
DF_IMG=${DF_IMG:-docker.dragonflydb.io/dragonflydb/dragonfly:v2.0.2}
KVROCKS_IMG=${KVROCKS_IMG:-apache/kvrocks:2.17.0}
sudo apt-get update -qq
sudo apt-get install -y -qq build-essential cmake clang pkg-config libssl-dev git curl jq python3 python3-matplotlib \
  docker.io redis-tools valgrind numactl sysstat linux-tools-common "linux-tools-$(uname -r)" > /dev/null
sudo usermod -aG docker "$USER"
sudo systemctl start docker
echo "* soft nofile 65535" | sudo tee -a /etc/security/limits.conf > /dev/null
echo "* hard nofile 65535" | sudo tee -a /etc/security/limits.conf > /dev/null
echo 'kernel.perf_event_paranoid = -1' | sudo tee /etc/sysctl.d/99-perf.conf > /dev/null
echo 'kernel.kptr_restrict = 0' | sudo tee -a /etc/sysctl.d/99-perf.conf > /dev/null
echo 'vm.overcommit_memory = 1' | sudo tee -a /etc/sysctl.d/99-perf.conf > /dev/null
echo 'net.core.somaxconn = 65535' | sudo tee -a /etc/sysctl.d/99-perf.conf > /dev/null
sudo sysctl -q --system
mkdir -p "$HOME/bin" "$HOME/out"
sudo ln -sf /usr/bin/python3 /usr/local/bin/python   # scripts/perf/run.sh calls python
if [ ! -x "$HOME/.cargo/bin/cargo" ]; then
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal > /dev/null
fi
if [ ! -x "$HOME/bin/redis-server" ]; then
  (cd /tmp && curl -sSfL "https://github.com/redis/redis/archive/refs/tags/$REDIS_VER.tar.gz" -o r.tgz \
    && tar xzf r.tgz && cd "redis-$REDIS_VER" && make -j16 > /tmp/redis-build.log 2>&1 \
    && cp src/redis-server src/redis-cli "$HOME/bin/")
fi
"$HOME/bin/redis-server" --version
if [ ! -x "$HOME/bin/kividb-1.1.3" ]; then
  (cd /tmp && curl -sSfL https://github.com/kividbio/kividb/releases/download/v1.1.3/kividb-linux-x86_64.tar.gz -o k113.tgz \
    && mkdir -p k113 && tar xzf k113.tgz -C k113 && cp "$(find k113 -type f -name kividb | head -1)" "$HOME/bin/kividb-1.1.3")
fi
"$HOME/bin/kividb-1.1.3" --version
[ -d "$HOME/kividb" ] || git clone -q https://github.com/kividbio/kividb "$HOME/kividb"
[ -d "$HOME/FlameGraph" ] || git clone -q --depth 1 https://github.com/brendangregg/FlameGraph "$HOME/FlameGraph"
for img in "$VALKEY_IMG" "$DF_IMG" "$KVROCKS_IMG"; do sudo docker pull -q "$img" > /dev/null; done
sudo mkdir -p /mnt/kvrocks && sudo chmod 777 /mnt/kvrocks
nproc; free -g | sed -n 2p; uname -r; lscpu | grep 'Model name'
echo SETUP-OK
