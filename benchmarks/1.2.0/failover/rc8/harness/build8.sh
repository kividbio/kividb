#!/usr/bin/env bash
# build8.sh (on bld, under nohup): build KiviDB 082dab3d (release/1.2.0, reports 1.2.0-rc8) from source with
# `cargo build --release` on the stable toolchain, record the SHA-256, copy the binary to /opt/kv/kividb here and on
# every VM in ~/hosts.txt (private IPs), then start bld8.sh (items 3-5).
set -u
exec > ~/build.log 2>&1
until grep -q BASE-OK ~/base.log 2>/dev/null; do sleep 5; done
curl -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain stable > /dev/null
. ~/.cargo/env; rustc --version; cargo --version
git clone -q https://github.com/kividbio/kividb ~/kividb && cd ~/kividb && git fetch -q origin release/1.2.0
git checkout -q 082dab3d8929c180bccf993835e28992ea054fb5 && git log -1 --format='%H %s'
git branch -r --contains HEAD
grep -m1 '^version' Cargo.toml
t0=$(date +%s)
cargo build --release 2>&1 | tail -5; [ -x target/release/kividb ] || { echo BUILD-FAILED; exit 1; }
echo "build took $(( $(date +%s) - t0 )) s"
install -m 755 target/release/kividb /opt/kv/kividb
/opt/kv/kividb --version
sha256sum /opt/kv/kividb | tee ~/kividb.sha256
for ip in $(cat ~/hosts.txt); do
  for i in 1 2 3 4 5; do scp -q /opt/kv/kividb azureuser@$ip:/tmp/kividb && ssh azureuser@$ip 'until [ -d /opt/kv ] && [ -w /opt/kv ]; do sleep 3; done; install -m 755 /tmp/kividb /opt/kv/kividb && sha256sum /opt/kv/kividb' && break; sleep 10; done
done
echo BUILD-DONE
nohup bash ~/fo/harness/bld8.sh > ~/bld8.log 2>&1 < /dev/null &
