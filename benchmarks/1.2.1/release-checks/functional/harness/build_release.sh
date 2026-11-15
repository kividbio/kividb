#!/usr/bin/env bash
# build_release.sh (on fp): data disk at /data, Docker, then the release.yml linux-amd64 build of 35a413c0
# (amazonlinux:2023 container, rustup stable, cargo build --release: fat LTO, codegen-units 1 per Cargo.toml)
# for the default and full (lua,tls) variants, in parallel, each in its own copy of the tree.
set -x
exec > ~/build.log 2>&1
DATA=$(readlink -f /dev/disk/azure/scsi1/lun0)   # the P30 data disk (sda on this VM, not sdc)
mountpoint -q /data || { sudo mkfs.ext4 -q -F $DATA && sudo mkdir -p /data && sudo mount -o noatime $DATA /data && sudo chown azureuser /data; } || exit 1
sudo DEBIAN_FRONTEND=noninteractive apt-get update -q
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -q docker.io cmake clang build-essential pkg-config python3-venv bpftrace strace fio sysstat redis-tools bc jq
for v in default full; do
  mkdir -p /data/b-$v && tar xzf ~/comb.tgz -C /data/b-$v
  (cd /data/b-$v && git init -q && git add -A && git add -f tests/fixtures tests/e2e/requirements.txt && git -c user.email=t@t -c user.name=t commit -qm src)
done
b() { local v=$1 f=$2; local s=$(date +%s)
  sudo docker run --rm -v /data/b-$v:/workspace -w /workspace -e FEATURES="$f" amazonlinux:2023 bash -c '
    set -euo pipefail
    dnf install -y gcc gcc-c++ make cmake perl git openssl-devel pkgconfig nasm >/dev/null
    curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs -o /tmp/rustup.sh
    sh /tmp/rustup.sh -y --profile minimal --default-toolchain stable
    source "$HOME/.cargo/env"
    rustc --version
    cargo build --release ${FEATURES:+--features "${FEATURES}"}
  ' > ~/build-$v.log 2>&1
  echo "build $v rc=$? $(( $(date +%s) - s ))s"; }
b default "" &
b full "lua,tls" &
wait
mkdir -p ~/bin
sudo cp /data/b-default/target/release/kividb ~/bin/kividb-default; sudo cp /data/b-full/target/release/kividb ~/bin/kividb-full
sudo chown azureuser ~/bin/*
~/bin/kividb-default --version; ~/bin/kividb-full --version
(cd ~/bin && sha256sum kividb-default kividb-full | tee ~/sha256-35a413c0.txt)
grep -h "rustc " ~/build-*.log
echo BUILD-DONE
