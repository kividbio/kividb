#!/usr/bin/env bash
# build.sh: on cl-cc. Build commit 878746e4 (default variant) with rustup stable on the host.
set -eu
export DEBIAN_FRONTEND=noninteractive
sudo apt-get update -qq && sudo apt-get install -y -qq build-essential cmake clang pkg-config redis-tools python3-venv jq > /dev/null
mkdir -p ~/src && cd ~/src && tar xzf ~/src.tgz
git init -q && git add -A && git add -f tests/fixtures tests/e2e/requirements.txt && git -c user.email=t@t -c user.name=t commit -qm src
curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs -o /tmp/rustup.sh
sh /tmp/rustup.sh -y --profile minimal --default-toolchain stable > /dev/null
. "$HOME/.cargo/env"
rustc --version
t=$(date +%s)
cargo build --release 2>&1 | tail -3
echo "build seconds $(( $(date +%s) - t ))"
./target/release/kividb --version
sha256sum target/release/kividb
python3 -m venv ~/venv && ~/venv/bin/pip -q install redis==5.2.1
echo BUILD-DONE
