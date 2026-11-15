#!/bin/bash
# Benchmark client: memtier_benchmark built from its latest release tag, and
# redis-cli. Runs no benchmark: scripts/run-matrix.sh does.
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive

exec > >(tee /var/log/user-data-client.log) 2>&1

# Set to a tag to pin memtier; empty builds its newest release tag. The
# version built is printed below and recorded with every run.
MEMTIER_REF="2.5.2"
BENCHMARK_REPO="https://github.com/kividbio/kivi-benchmark"

apt-get update -y
apt-get install -y build-essential autoconf automake libpcre3-dev \
  libevent-dev pkg-config zlib1g-dev libssl-dev git redis-tools python3

cat >> /etc/security/limits.conf << 'LIMITS'
* soft nofile 1000000
* hard nofile 1000000
LIMITS

sudo -u ubuntu -H bash << EOSU
set -euo pipefail
cd /home/ubuntu
[[ -d memtier_benchmark ]] || git clone https://github.com/RedisLabs/memtier_benchmark
cd memtier_benchmark
git fetch --tags --quiet
ref="${MEMTIER_REF}"
[[ -n "\$ref" ]] || ref=\$(git describe --tags "\$(git rev-list --tags --max-count=1)")
git checkout --quiet "\$ref"
autoreconf -ivf
./configure
make -j"\$(nproc)"
cd /home/ubuntu
mkdir -p kivi-benchmark/scripts
EOSU

cd /home/ubuntu/memtier_benchmark && make install

echo "Client user-data finished. $(memtier_benchmark --version 2>&1 | head -1)"
echo "Run a matrix with ~/kivi-benchmark/scripts/run-matrix.sh."
