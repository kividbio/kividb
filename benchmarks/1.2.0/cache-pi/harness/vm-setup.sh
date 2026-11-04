#!/bin/bash
# vm-setup.sh server|client|builder -- packages for each role (Ubuntu 24.04 arm64).
set -ex
ROLE=$1
sudo apt-get update -q
case $ROLE in
server)
  sudo DEBIAN_FRONTEND=noninteractive apt-get install -yq redis-tools sysstat
  mkdir -p ~/bin ~/run
  ;;
client)
  sudo DEBIAN_FRONTEND=noninteractive apt-get install -yq redis-tools sysstat git curl time
  cd ~
  curl -sSfL -o go.tgz https://go.dev/dl/go1.25.3.linux-arm64.tar.gz
  sudo rm -rf /usr/local/go && sudo tar -C /usr/local -xzf go.tgz
  export PATH=/usr/local/go/bin:$PATH
  rm -rf bench && git clone -q https://github.com/imchuncai/umem-cache-benchmark bench
  # the harness commit used for the 2026-11-01 run posted on kividbio/kividb#202
  cd bench && git checkout -q 6d77492 && git log -1 --format='%H %cd %s' > ~/harness-commit.txt
  go version >> ~/harness-commit.txt
  go test -c -o ~/bench.test . 2>&1 | tail -3
  ls -la ~/bench.test
  mkdir -p ~/results
  ;;
builder)
  sudo DEBIAN_FRONTEND=noninteractive apt-get install -yq build-essential pkg-config libssl-dev git curl clang cmake
  curl -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal
  mkdir -p ~/bin
  ;;
esac
echo SETUP-DONE
