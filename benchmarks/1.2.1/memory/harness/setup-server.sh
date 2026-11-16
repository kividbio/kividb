#!/bin/bash
# Engine VM, 1.2.1 round. Formats and mounts the local NVMe disk for data files, then prepares the two builds:
#   kividb-1.2.0  the released binary: kividb-linux-x86_64.tar.gz from the v1.2.0 GitHub release, its SHA-256
#                 checked against the release's SHA256SUMS.txt
#   kividb-1.2.1  commit 878746e4, built the way .github/workflows/release.yml builds the release binary: inside an
#                 amazonlinux:2023 container, rustup stable (minimal profile), `cargo build --release`, default
#                 features (the release profile: lto fat, codegen-units 1)
# Each binary is wrapped in the same minimal image (ubuntu:24.04 + the binary, ENTRYPOINT kividb, run as root) so
# that eng.sh and warefeats' compose files start both the same way.
#   setup-server.sh /var/tmp/src-878746e4.tgz
# Writes /var/tmp/<name>-image (the tag eng.sh runs), -version, -sha256, -kividb (the binary), and for 1.2.1 -rustc.
set -euxo pipefail
SRC_TGZ=$1
if ! mountpoint -q /mnt/nvme; then
  DEV=$(lsblk -dpno NAME,TYPE | awk '$2=="disk"{print $1}' | while read -r d; do
    [ "$(lsblk -no NAME "$d" | wc -l)" = 1 ] && ! findmnt -S "$d" >/dev/null && echo "$d"; done | sed -n 1p)
  mkfs.ext4 -F -q -E nodiscard "$DEV"
  mkdir -p /mnt/nvme && mount -o noatime "$DEV" /mnt/nvme
fi
mkdir -p /mnt/nvme/data && chmod 777 /mnt/nvme/data
df -h /mnt/nvme

# 1.2.0: the release tarball, checked
mkdir -p /var/tmp/rel120 && cd /var/tmp/rel120
U=https://github.com/kividbio/kividb/releases/download/v1.2.0
curl -fsSLO "$U/kividb-linux-x86_64.tar.gz"
curl -fsSLO "$U/SHA256SUMS.txt"
grep -E ' (./)?kividb-linux-x86_64.tar.gz$' SHA256SUMS.txt | sed 's/^\([0-9a-f]*\) .*/\1  kividb-linux-x86_64.tar.gz/' | sha256sum -c - | tee /var/tmp/kividb-1.2.0-tarball-check
tar xzf kividb-linux-x86_64.tar.gz
cp kividb/kividb /var/tmp/kividb-1.2.0-kividb
sha256sum kividb-linux-x86_64.tar.gz | awk '{print $1}' > /var/tmp/kividb-1.2.0-tarball-sha256

# 1.2.1: release.yml's build
rm -rf /opt/kividb && mkdir -p /opt/kividb && cd /opt/kividb
tar xzf "$SRC_TGZ"
git init -q && git add -A && git add -f tests/fixtures tests/e2e/requirements.txt && git -c user.email=t@t -c user.name=t commit -qm src
docker run --rm -v "$PWD:/workspace" -w /workspace amazonlinux:2023 bash -c '
  set -euo pipefail
  dnf install -y gcc gcc-c++ make cmake perl git openssl-devel pkgconfig nasm >/dev/null
  curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs -o /tmp/rustup.sh
  sh /tmp/rustup.sh -y --profile minimal --default-toolchain stable
  source "$HOME/.cargo/env"
  rustc --version > /workspace/rustc-version.txt
  cargo build --release
' > /var/tmp/kividb-1.2.1-build.log 2>&1
cp target/release/kividb /var/tmp/kividb-1.2.1-kividb
cp rustc-version.txt /var/tmp/kividb-1.2.1-rustc

for NAME in kividb-1.2.0 kividb-1.2.1; do
  D=/var/tmp/img-$NAME; rm -rf "$D"; mkdir -p "$D"
  cp "/var/tmp/$NAME-kividb" "$D/kividb"
  printf 'FROM ubuntu:24.04\nCOPY kividb /usr/local/bin/kividb\nENTRYPOINT ["kividb"]\nCMD ["--protected-mode", "no"]\n' > "$D/Dockerfile"
  docker build -q -t "kividb/kividb:bin-$NAME" "$D"
  echo "kividb/kividb:bin-$NAME" > "/var/tmp/$NAME-image"
  docker run --rm "kividb/kividb:bin-$NAME" --version > "/var/tmp/$NAME-version" 2>&1 || true
  sha256sum "/var/tmp/$NAME-kividb" | awk '{print $1}' > "/var/tmp/$NAME-sha256"
done
docker images --digests > /var/tmp/engine-images.txt
uname -a > /var/tmp/kernel.txt; lscpu > /var/tmp/lscpu.txt; free -g > /var/tmp/free.txt
touch /var/tmp/engine-ready
