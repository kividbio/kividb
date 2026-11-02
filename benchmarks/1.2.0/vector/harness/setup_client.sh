#!/usr/bin/env bash
# Client: the two vector-db-benchmark pins vector-benchmark.yml builds
#   ~/vdb   redis/vector-db-benchmark 31a28b5  (glove-25-angular, its own baseline pin)
#   ~/vdbx  redis/vector-db-benchmark d22d44e  (tag 0.2.0; filtered + dbpedia datasets)
# and the engines-files the workflow writes (copied verbatim, plus the same
# file once more for dbpedia-1M with the same settings).
set -euo pipefail
REF=31a28b5ae6d35da96de6d218ada209868a628b42
REF_EXTRA=d22d44e8b46009a800a59e952710166ece2e38a1
export DEBIAN_FRONTEND=noninteractive
sudo apt-get update -qq
sudo apt-get install -y -qq build-essential libhdf5-dev pkg-config dpkg-dev git sysstat
if ! command -v cargo >/dev/null; then
  curl -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal >/dev/null
fi
. ~/.cargo/env
HDF5_DIR="/usr/lib/$(dpkg-architecture -qDEB_HOST_MULTIARCH)/hdf5/serial"; export HDF5_DIR
for pair in "vdb $REF" "vdbx $REF_EXTRA"; do
  set -- $pair
  d=$1 ref=$2
  rm -rf ~/src-$d && git clone -q https://github.com/redis/vector-db-benchmark ~/src-$d
  (cd ~/src-$d && git checkout -q $ref && cargo build -q --release --bin vector-db-benchmark)
  mkdir -p ~/$d/datasets
  cp ~/src-$d/target/release/vector-db-benchmark ~/$d/vector-db-benchmark
  cp ~/src-$d/datasets/datasets.json ~/$d/datasets/datasets.json
done

for e in kividb redis dragonfly; do
  cat > ~/vdb/$e-engine.json << ENGINES
[
  {
    "name": "$e-glove25",
    "engine": "$e",
    "connection_params": {},
    "collection_params": { "hnsw_config": { "M": 16, "EF_CONSTRUCTION": 256 } },
    "search_params": [
      { "parallel": 100, "search_params": { "ef": 64 } },
      { "parallel": 100, "search_params": { "ef": 128 } },
      { "parallel": 100, "search_params": { "ef": 256 } },
      { "parallel": 100, "search_params": { "ef": 512 } }
    ],
    "upload_params": { "parallel": 100, "batch_size": 64 }
  }
]
ENGINES
  cat > ~/vdbx/$e-engine.json << ENGINES
[
  {
    "name": "$e-m16-ef128",
    "engine": "$e",
    "connection_params": {},
    "collection_params": { "hnsw_config": { "M": 16, "EF_CONSTRUCTION": 128 } },
    "search_params": [
      { "parallel": 1, "search_params": { "ef": 64 } },
      { "parallel": 1, "search_params": { "ef": 128 } },
      { "parallel": 1, "search_params": { "ef": 256 } },
      { "parallel": 16, "search_params": { "ef": 128 } },
      { "parallel": 100, "search_params": { "ef": 128 } }
    ],
    "upload_params": { "parallel": 16, "batch_size": 64 }
  }
]
ENGINES
done
~/vdbx/vector-db-benchmark --describe build 2>/dev/null | head -20 || true
echo SETUP-CLIENT-OK
