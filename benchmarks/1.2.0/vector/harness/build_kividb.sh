#!/usr/bin/env bash
# Build one KiviDB ref the way vector-benchmark.yml and the arm64 release do:
# default features, release profile, LSE atomics.
# Usage: build_kividb.sh <git ref> <name>   -> ~/bins/<name>/kividb, ~/bins/<name>/commit
set -euo pipefail
REF=$1 NAME=$2
. ~/.cargo/env
[ -d ~/kividb-src ] || git clone -q https://github.com/kividbio/kividb ~/kividb-src
cd ~/kividb-src
git fetch -q --tags origin
git checkout -q --detach "$REF"
RUSTFLAGS="-C target-feature=+lse" cargo build --release 2>&1 | tail -3
mkdir -p ~/bins/$NAME
cp target/release/kividb ~/bins/$NAME/kividb
git rev-parse HEAD > ~/bins/$NAME/commit
~/bins/$NAME/kividb --version || true
echo "BUILT $NAME $(cat ~/bins/$NAME/commit)"
