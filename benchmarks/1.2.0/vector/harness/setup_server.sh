#!/usr/bin/env bash
# Server: Redis 8.10.2 (query engine module from the redis apt package, the
# vector-benchmark.yml pin) and Dragonfly v2.0.2 native arm64. KiviDB is built
# on a separate builder VM with build_kividb.sh and copied to ~/bins/<name>/,
# so nothing compiles on the server.
set -euo pipefail
REDIS_DEB="${REDIS_DEB:-6:8.10.2-1rl1~jammy1}"
DRAGONFLY_VERSION="${DRAGONFLY_VERSION:-v2.0.2}"
export DEBIAN_FRONTEND=noninteractive
sudo apt-get update -qq
sudo apt-get install -y -qq ca-certificates curl gnupg lsb-release git sysstat linux-tools-common "linux-tools-$(uname -r)" || \
  sudo apt-get install -y -qq ca-certificates curl gnupg lsb-release git sysstat
curl -fsSL https://packages.redis.io/gpg | sudo gpg --yes --dearmor -o /usr/share/keyrings/redis-archive-keyring.gpg
echo "deb [signed-by=/usr/share/keyrings/redis-archive-keyring.gpg arch=arm64] https://packages.redis.io/deb $(lsb_release -cs) main" \
  | sudo tee /etc/apt/sources.list.d/redis.list
sudo apt-get update -qq
sudo apt-get install -y -qq --allow-downgrades "redis=${REDIS_DEB}" "redis-server=${REDIS_DEB}" "redis-tools=${REDIS_DEB}"
MOD=$(dpkg -L redis redis-server redis-tools 2>/dev/null | grep -m1 '/redisearch\.so$')
echo "$MOD" > ~/redisearch-path
sudo systemctl disable --now redis-server || true
cd ~
wget -q "https://github.com/dragonflydb/dragonfly/releases/download/${DRAGONFLY_VERSION}/dragonfly-aarch64.tar.gz" -O df.tgz
tar -xzf df.tgz && chmod +x dragonfly-aarch64 && rm -f df.tgz
./dragonfly-aarch64 --version | head -2
redis-server --version
echo SETUP-SERVER-OK
