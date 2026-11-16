#!/usr/bin/env bash
# Runs on the BUILDER: builds the default variant exactly as release.yml's linux-arm64 job does
# (inside amazonlinux:2023, rustup stable, RUSTFLAGS=-C target-feature=+lse, cargo build --release;
# Cargo.toml's release profile is lto = "fat", codegen-units = 1), then packs it like release.yml.
set -euo pipefail
exec > ~/build.log 2>&1
sudo apt-get update -qq && sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq docker.io git
mkdir -p ~/src && tar -xzf ~/comb.tgz -C ~/src && cd ~/src && git init -q && git add -A && git add -f tests/fixtures tests/e2e/requirements.txt && git -c user.email=t@t -c user.name=t commit -qm src
sudo docker run --rm -v ~/src:/src -w /src amazonlinux:2023 bash -c '
  set -e
  dnf install -y gcc gcc-c++ make cmake perl git openssl-devel pkgconfig >/dev/null
  curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs -o /tmp/rustup.sh
  sh /tmp/rustup.sh -y --profile minimal --default-toolchain stable
  . "$HOME/.cargo/env"
  rustc --version > /src/rustc-version.txt
  export RUSTFLAGS="-C target-feature=+lse"
  cargo build --release
  chown -R '"$(id -u):$(id -g)"' /src/target /src/rustc-version.txt'
mkdir -p ~/stage/kividb && cp ~/src/target/release/kividb ~/stage/kividb/ && cp ~/src/LICENSE ~/src/NOTICE ~/stage/kividb/ 2>/dev/null || true
tar czf ~/kividb-linux-aarch64.tar.gz -C ~/stage kividb
sha256sum ~/kividb-linux-aarch64.tar.gz ~/stage/kividb/kividb
cat ~/src/rustc-version.txt
echo BUILD-OK
