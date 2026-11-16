#!/usr/bin/env bash
# inst.sh [tag]: install the default-variant release binary from the ORAS artifact into /opt/kv, checking SHA256SUMS
set -eu
T=${1:-v1.2.0-rc7}
d=$(mktemp -d); cd "$d"
oras pull "ghcr.io/kividbio/kividb-binaries:$T" > ~/oras-$T.txt
sha256sum -c --ignore-missing SHA256SUMS.txt > ~/sha-$T.txt 2>&1
grep kividb-linux-x86_64.tar.gz ~/sha-$T.txt
tar xzf kividb-linux-x86_64.tar.gz
install -m 755 kividb/kividb /opt/kv/kividb
/opt/kv/kividb --version
sha256sum /opt/kv/kividb
ls > ~/oras-files-$T.txt
cd; rm -rf "$d"
