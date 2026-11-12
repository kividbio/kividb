#!/usr/bin/env bash
# On the server: build KiviDB 1.2.1 (35a413c0) from the copied tree the way release.yml builds the
# linux-aarch64 default variant (amazonlinux:2023, rustup stable, +lse, cargo build --release with
# the repo's fat-LTO release profile), and check the 1.2.0 release binary against SHA256SUMS.txt.
# Same as the earlier rerun's build-121.sh, except the source comes from ~/comb.tgz (scp'd), not git.
#
#   build-c3.sh     (expects ~/comb.tgz)
set -euo pipefail
cd "$HOME"
until [ -f /var/log/user-data-server.log ] && grep -q 'Server user-data finished' /var/log/user-data-server.log; do sleep 10; done
sudo apt-get install -y docker.io > /dev/null
sudo systemctl start docker
rm -rf src && mkdir src && tar -xzf comb.tgz -C src
cd src && git init -q && git add -A && git add -f tests/fixtures tests/e2e/requirements.txt && git -c user.email=t@t -c user.name=t commit -qm src
grep -A3 '^\[profile.release\]' Cargo.toml
sudo docker run --rm -v "$PWD:/workspace" -w /workspace amazonlinux:2023 bash -c '
  set -euo pipefail
  dnf install -y gcc gcc-c++ make cmake perl git openssl-devel pkgconfig >/dev/null
  curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs -o /tmp/rustup.sh
  sh /tmp/rustup.sh -y --profile minimal --default-toolchain stable >/dev/null
  source "$HOME/.cargo/env"
  rustc --version
  export RUSTFLAGS="-C target-feature=+lse"
  cargo build --release 2>&1 | tail -3
'
mkdir -p "$HOME/bin121"
install -m 0755 target/release/kividb "$HOME/bin121/kividb"
cd "$HOME"
curl -fsSL https://github.com/kividbio/kividb/releases/download/v1.2.0/SHA256SUMS.txt -o SHA256SUMS-120.txt
curl -fsSL https://github.com/kividbio/kividb/releases/download/v1.2.0/kividb-linux-aarch64.tar.gz -o k120.tgz
grep ' ./kividb-linux-aarch64.tar.gz$' SHA256SUMS-120.txt
sha256sum k120.tgz
mkdir -p k120 && tar -xzf k120.tgz -C k120
echo "release tarball binary vs ~/bin/kividb:"
sha256sum k120/kividb/kividb bin/kividb bin121/kividb
bin/kividb --version | head -1
bin121/kividb --version | head -1
uname -a
echo BUILD-DONE
