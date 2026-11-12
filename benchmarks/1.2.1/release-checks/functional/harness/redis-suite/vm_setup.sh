#!/usr/bin/env bash
# Runs on the VM: deps, rustup, source tree, builds of the variants given as args.
set -ux
sudo apt-get update -qq
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq build-essential cmake clang pkg-config tcl tcllib redis-tools python3-venv python3-pip openssl jq git >/dev/null
[ -x ~/.cargo/bin/cargo ] || curl -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal >/dev/null
. ~/.cargo/env
mkdir -p ~/src && cd ~/src && tar xzf ~/comb.tgz
git init -q && git add -A && git add -f tests/fixtures tests/e2e/requirements.txt && git -c user.email=t@t -c user.name=t commit -qm src
export CARGO_PROFILE_RELEASE_LTO=off
mkdir -p ~/bin
for v in "$@"; do
  case $v in default) f="";; *) f="--features $v";; esac
  name=$(echo $v | tr ',' '+')
  CARGO_TARGET_DIR=~/tgt-$name cargo build --release $f > ~/build-$name.log 2>&1 && cp ~/tgt-$name/release/kividb ~/bin/kividb-$name && echo "built $name"
done
echo SETUP-DONE
