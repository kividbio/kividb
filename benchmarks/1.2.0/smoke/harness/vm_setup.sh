#!/usr/bin/env bash
# vm_setup.sh: prepare a fresh Ubuntu 24.04 VM (amd64 or arm64) for the smoke harness.
# Docker (+ buildx), redis-tools, openssl, jq, ORAS, gh-less. Then the repo at release/1.2.0.
set -eux
export DEBIAN_FRONTEND=noninteractive
sudo apt-get update -q
sudo apt-get install -y -q docker.io docker-buildx redis-tools openssl jq curl git netcat-openbsd python3-venv file
sudo usermod -aG docker "$USER"
ARCH=$(dpkg --print-architecture)
ORAS=1.2.2
curl -fsSL "https://github.com/oras-project/oras/releases/download/v$ORAS/oras_${ORAS}_linux_${ARCH}.tar.gz" | sudo tar xz -C /usr/local/bin oras
oras version
[ -d "$HOME/src" ] || git clone -q --branch release/1.2.0 https://github.com/kividbio/kividb "$HOME/src"
git -C "$HOME/src" log -1 --oneline
redis-cli --version; docker --version
echo SETUP-DONE
