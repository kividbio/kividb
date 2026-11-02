#!/usr/bin/env bash
# Build one KiviDB ref the way vector-benchmark.yml and the arm64 release do:
# default features, release profile, LSE atomics.
# Runs on the BUILDER VM (same image as the server); copy ~/bins/<name>/ to
# the server afterwards, so nothing compiles on the server.
# Usage: build_kividb.sh <git ref> <name>   -> ~/bins/<name>/{kividb,commit,rustc}
set -euo pipefail
REF=$1 NAME=$2
if [ ! -f ~/.cargo/env ]; then
  sudo DEBIAN_FRONTEND=noninteractive apt-get update -qq
  sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq build-essential cmake clang pkg-config git
  curl -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal >/dev/null
fi
. ~/.cargo/env
[ -d ~/kividb-src ] || git clone -q https://github.com/kividbio/kividb ~/kividb-src
cd ~/kividb-src
git fetch -q --tags origin
git fetch -q origin "$REF" 2>/dev/null || true
git checkout -q --detach "$REF"
RUSTFLAGS="-C target-feature=+lse" cargo build --release 2>&1 | tail -3
mkdir -p ~/bins/$NAME
cp target/release/kividb ~/bins/$NAME/kividb
git rev-parse HEAD > ~/bins/$NAME/commit
rustc --version > ~/bins/$NAME/rustc
~/bins/$NAME/kividb --version || true
echo "BUILT $NAME $(cat ~/bins/$NAME/commit)"
