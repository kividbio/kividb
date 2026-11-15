#!/usr/bin/env bash
# builds.sh: on the server VM, the two binaries under test, into ~/bin.
#   kividb-120  the released v1.2.0 x86_64 tarball from GitHub, checked against SHA256SUMS.txt
#   kividb-121  commit 878746e4 (tree in ~/src-878746e4.tgz), built the way .github/workflows/release.yml
#               builds the default x86_64 tarball: in an amazonlinux:2023 container, rustup stable
#               (--profile minimal), `cargo build --release` with no features and no RUSTFLAGS
#               (Cargo.toml's release profile: opt-level 3, lto fat, codegen-units 1).
# Writes ~/build.log.
set -euo pipefail
cd ~
{
  # 1.2.0 release binary
  rm -rf rel120; mkdir rel120; cd rel120
  base=https://github.com/kividbio/kividb/releases/download/v1.2.0
  curl -sSfLO "$base/kividb-linux-x86_64.tar.gz"
  curl -sSfLO "$base/SHA256SUMS.txt"
  echo "SHA256SUMS line: $(grep -E '[ *](\./)?kividb-linux-x86_64\.tar\.gz$' SHA256SUMS.txt)"
  grep -E '[ *](\./)?kividb-linux-x86_64\.tar\.gz$' SHA256SUMS.txt | sha256sum -c -
  tar xzf kividb-linux-x86_64.tar.gz
  cp kividb/kividb ~/bin/kividb-120
  cd ~
  # 1.2.1 at 878746e4, release.yml's way
  rm -rf src121; mkdir src121; tar xzf src-878746e4.tgz -C src121
  docker run --rm -v "$HOME/src121:/workspace" -w /workspace amazonlinux:2023 bash -c '
    set -euo pipefail
    dnf install -y gcc gcc-c++ make cmake perl git openssl-devel pkgconfig nasm >/dev/null
    curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs -o /tmp/rustup.sh
    sh /tmp/rustup.sh -y --profile minimal --default-toolchain stable >/dev/null 2>&1
    source "$HOME/.cargo/env"
    rustc --version; cargo --version; ldd --version | head -1
    cargo build --release 2>&1 | tail -2
  '
  cp src121/target/release/kividb ~/bin/kividb-121
  echo "kividb-120: $(~/bin/kividb-120 --version)"
  echo "kividb-121: $(~/bin/kividb-121 --version)"
  sha256sum ~/bin/kividb-120 ~/bin/kividb-121
  echo BUILD-DONE
} > ~/build.log 2>&1
