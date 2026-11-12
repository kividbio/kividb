#!/bin/bash
# Client VM: ghcr.io/warefeats/memtier-benchmark is not public, so build it from the warefeats repo's
# Dockerfile.memtier (memtier_benchmark 2.5.1) under the tag their runner expects.
set -euxo pipefail
cd /opt/bench
docker build -t ghcr.io/warefeats/memtier-benchmark:2.5.1 -f Dockerfile.memtier . > /var/tmp/memtier-build.log 2>&1
docker run --rm ghcr.io/warefeats/memtier-benchmark:2.5.1 memtier_benchmark --version
uname -a > /var/tmp/kernel.txt; lscpu > /var/tmp/lscpu.txt
touch /var/tmp/client-ready
