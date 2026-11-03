#!/usr/bin/env bash
# build.sh [commit]: build the default variant of kividb from source on this VM, the way release.yml's linux-amd64 job
# does: inside an amazonlinux:2023 container, rustup stable (minimal profile), `cargo build --release`, no features.
# rc8 is not tagged; the soak builds commit 082dab3d on branch release/1.2.0 of github.com/kividbio/kividb.
# Leaves the binary at ~/kividb-build/kividb and prints its sha256 and --version.
set -eu
C=${1:-082dab3d8929c180bccf993835e28992ea054fb5}
export DEBIAN_FRONTEND=noninteractive
sudo apt-get install -y -qq docker.io git > /dev/null
sudo systemctl start docker
cd ~; rm -rf src; git clone --quiet --branch release/1.2.0 https://github.com/kividbio/kividb src; cd src
git checkout --quiet "$C"
git log -1 --format='%H %ci %s' > ~/build-commit.txt
grep -m1 '^version' Cargo.toml >> ~/build-commit.txt
t=$(date +%s)
sudo docker run --rm -v "$PWD:/workspace" -w /workspace amazonlinux:2023 bash -c '
  set -euo pipefail
  dnf install -y gcc gcc-c++ make cmake perl git openssl-devel pkgconfig nasm >/dev/null
  curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs -o /tmp/rustup.sh
  sh /tmp/rustup.sh -y --profile minimal --default-toolchain stable
  source "$HOME/.cargo/env"
  rustc --version; cargo --version
  cargo build --release
' > ~/build.log 2>&1
echo "build seconds $(( $(date +%s) - t ))" >> ~/build-commit.txt
grep -E '^rustc|^cargo ' ~/build.log >> ~/build-commit.txt
mkdir -p ~/kividb-build
sudo install -m 755 -o azureuser target/release/kividb ~/kividb-build/kividb
sudo systemctl stop docker docker.socket || true
~/kividb-build/kividb --version | tee -a ~/build-commit.txt
sha256sum ~/kividb-build/kividb | tee -a ~/build-commit.txt
