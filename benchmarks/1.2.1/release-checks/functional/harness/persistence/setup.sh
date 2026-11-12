#!/usr/bin/env bash
# setup.sh: data disk (premium SSD P30, 1 TiB, 5000 IOPS, 200 MB/s, host caching None) at /data, toolchain, both builds.
set -x
sudo mkfs.ext4 -q -F /dev/sdc && sudo mkdir -p /data && sudo mount -o noatime /dev/sdc /data && sudo chown azureuser /data
sudo DEBIAN_FRONTEND=noninteractive apt-get update -q && sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -q cmake clang build-essential pkg-config python3-venv bpftrace strace fio sysstat
curl -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal
. ~/.cargo/env
mkdir -p ~/src ~/bin && cd ~/src && tar xzf ~/src-878746e4.tgz && git init -q && git add -A && git add -f tests/fixtures tests/e2e/requirements.txt && git -c user.email=t@t -c user.name=t commit -qm src
python3 -m venv ~/venv && ~/venv/bin/pip install -q redis
export CARGO_PROFILE_RELEASE_LTO=off
CARGO_TARGET_DIR=/data/tgt-default cargo build --release > ~/build-default.log 2>&1 && cp /data/tgt-default/release/kividb ~/bin/kividb-default
CARGO_TARGET_DIR=/data/tgt-full cargo build --release --features lua,tls > ~/build-full.log 2>&1 && cp /data/tgt-full/release/kividb ~/bin/kividb-full
ls -la ~/bin; ~/bin/kividb-default --version; ~/bin/kividb-full --version
echo SETUP-DONE
