#!/usr/bin/env bash
# build.sh <commit> <tag>: build the default variant from source the way release.yml's linux-amd64 job does: inside an
# amazonlinux:2023 container, rustup stable (minimal profile), `cargo build --release`, no features (the release
# profile: fat LTO). Leaves ~/kb-<tag>/kividb and ~/kb-<tag>/build.txt (commit, rustc, build time, --version, sha256).
set -eu
C=$1 T=$2
sudo systemctl start docker
cd ~; rm -rf src-$T; git clone --quiet --branch release/1.2.0 https://github.com/kividbio/kividb src-$T; cd src-$T
git checkout --quiet "$C"
mkdir -p ~/kb-$T; B=~/kb-$T/build.txt
git log -1 --format='%H %ci %s' > $B
grep -m1 '^version' Cargo.toml >> $B
t=$(date +%s)
sudo docker run --rm -v "$PWD:/workspace" -w /workspace amazonlinux:2023 bash -c '
  set -euo pipefail
  dnf install -y gcc gcc-c++ make cmake perl git openssl-devel pkgconfig nasm >/dev/null
  curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs -o /tmp/rustup.sh
  sh /tmp/rustup.sh -y --profile minimal --default-toolchain stable
  source "$HOME/.cargo/env"
  rustc --version; cargo --version
  cargo build --release
' > ~/kb-$T/build.log 2>&1
echo "build seconds $(( $(date +%s) - t ))" >> $B
grep -E '^rustc|^cargo ' ~/kb-$T/build.log >> $B
sudo install -m 755 -o azureuser target/release/kividb ~/kb-$T/kividb
~/kb-$T/kividb --version | tee -a $B
sha256sum ~/kb-$T/kividb | sed "s#$HOME/##" | tee -a $B
