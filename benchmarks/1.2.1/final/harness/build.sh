#!/usr/bin/env bash
# build.sh (srv): 74071531 built the way release.yml's linux-arm64 job builds it: amazonlinux:2023 container,
# dnf gcc/cmake/perl/openssl-devel, rustup stable (minimal), RUSTFLAGS=+lse, cargo build --release [--features],
# Cargo.toml's [profile.release] (fat LTO, codegen-units 1). Variants: default and full (lua,tls).
set -uo pipefail
VARIANTS=${VARIANTS:-default full}
exec > ~/builds-${VARIANTS// /_}.txt 2>&1
for v in ${VARIANTS:-default full}; do
  f=""; [ $v = full ] && f="lua,tls"
  echo "== build $v (features '$f')"
  t0=$(date +%s)
  sudo docker run --rm -v "$HOME/k121:/workspace" -v cargo-reg:/root/.cargo/registry -w /workspace -e FEATURES="$f" -e V=$v amazonlinux:2023 bash -c '
    set -euo pipefail
    dnf install -y gcc gcc-c++ make cmake perl git openssl-devel pkgconfig >/dev/null
    curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs -o /tmp/rustup.sh
    sh /tmp/rustup.sh -y --profile minimal --default-toolchain stable >/dev/null 2>&1
    source "$HOME/.cargo/env"
    echo "container: $(cat /etc/system-release); gcc $(gcc -dumpfullversion); glibc $(ldd --version | head -1)"
    rustc --version
    export RUSTFLAGS="-C target-feature=+lse" CARGO_TARGET_DIR=/workspace/target-$V
    cargo build --release ${FEATURES:+--features "${FEATURES}"} > /workspace/build-$V.log 2>&1 || { tail -40 /workspace/build-$V.log; exit 1; }
    tail -2 /workspace/build-$V.log
  ' || { echo "BUILD FAILED: $v"; continue; }
  sudo install -m 0755 -o azureuser ~/k121/target-$v/release/kividb ~/bin/kividb-121-$v
  echo "built $v in $(( $(date +%s) - t0 )) s"
done
cd ~/bin; for b in kividb-121-default kividb-121-full kividb-120-default kividb-120-full; do echo "$b: $(./$b --version 2>&1 | head -1)"; done
sha256sum kividb-*; file kividb-121-*
echo BUILDS-DONE
