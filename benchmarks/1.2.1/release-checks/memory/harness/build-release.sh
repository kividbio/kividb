#!/bin/bash
# Builds the tree under test exactly as .github/workflows/release.yml's linux-amd64 job does for the default
# variant (amazonlinux:2023 container, rustup stable, `cargo build --release`: the repo's profile is fat LTO,
# codegen-units 1) and stages kividb-linux-x86_64.tar.gz the same way (kividb/kividb + LICENSE + NOTICE).
#   build-release.sh <tree.tgz>      (needs docker; writes /var/tmp/build/dist/kividb-linux-x86_64.tar.gz)
set -euxo pipefail
TGZ=$1
B=/var/tmp/build; sudo rm -rf $B; mkdir -p $B/src && cd $B
tar -xzf "$TGZ" -C src && cd src
git init -q && git add -A && git add -f tests/fixtures tests/e2e/requirements.txt
git -c user.email=t@t -c user.name=t commit -qm src
sudo docker run --rm -v "${PWD}:/workspace" -w /workspace amazonlinux:2023 bash -c '
  set -euo pipefail
  dnf install -y gcc gcc-c++ make cmake perl git openssl-devel pkgconfig nasm >/dev/null
  curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs -o /tmp/rustup.sh
  sh /tmp/rustup.sh -y --profile minimal --default-toolchain stable
  source "$HOME/.cargo/env"
  rustc --version > /workspace/rustc-version.txt
  cargo build --release
'
mkdir -p $B/dist $B/staging/kividb
cp target/release/kividb $B/staging/kividb/kividb
cp LICENSE $B/staging/kividb/LICENSE; cp NOTICE $B/staging/kividb/NOTICE
tar czf $B/dist/kividb-linux-x86_64.tar.gz -C $B/staging kividb
cat rustc-version.txt > $B/dist/build-info.txt
sha256sum $B/dist/kividb-linux-x86_64.tar.gz $B/staging/kividb/kividb >> $B/dist/build-info.txt
cat $B/dist/build-info.txt
touch $B/dist/DONE
