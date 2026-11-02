#!/usr/bin/env bash
# install_engines.sh <kividb-tag> [redis-image]
# Puts the published KiviDB binary (default variant, from the release's ORAS artifact on GHCR) in /opt/kv and
# redis-server from the official Docker image in /opt/redis.
set -eu
TAG=$1 RIMG=${2:-redis:8.10.3}
ARCH=$(dpkg --print-architecture); case $ARCH in amd64) UN=x86_64 ;; arm64) UN=aarch64 ;; esac
d=$(mktemp -d); cd "$d"
oras pull "ghcr.io/kividbio/kividb-binaries:$TAG" >/dev/null
sha256sum -c --ignore-missing SHA256SUMS.txt >/dev/null
tar xzf "kividb-linux-$UN.tar.gz"
install -m 755 kividb/kividb /opt/kv/kividb
/opt/kv/kividb --version
docker pull -q "$RIMG" >/dev/null
cid=$(docker create "$RIMG")
docker cp "$cid:/usr/local/bin/redis-server" /opt/redis/redis-server
docker rm "$cid" >/dev/null
/opt/redis/redis-server --version
rm -rf "$d"
