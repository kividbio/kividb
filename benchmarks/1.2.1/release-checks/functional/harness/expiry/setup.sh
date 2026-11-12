#!/usr/bin/env bash
# On the VM: deps, rustup, tree (35a413c0), test binaries (release, LTO off, lua,tls), Redis 8.10.2 suite, venv.
# The server binaries in ~/bin are the release.yml builds copied from fp (not built here).
set -ux
exec > ~/setup.log 2>&1
sudo apt-get update -qq
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq build-essential cmake clang pkg-config tcl tcllib redis-tools python3-venv python3-pip openssl jq git sysstat >/dev/null
[ -x ~/.cargo/bin/cargo ] || curl -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal >/dev/null
. ~/.cargo/env
mkdir -p ~/src && cd ~/src && tar xzf ~/comb.tgz
git init -q && git add -A && git add -f tests/fixtures tests/e2e/requirements.txt && git -c user.email=t@t -c user.name=t commit -qm src
python3 -m venv ~/venv && ~/venv/bin/pip install -q redis hiredis
(cd ~ && git clone -q --depth 1 --branch 8.10.2 https://github.com/redis/redis.git redis-tests) &
export CARGO_PROFILE_RELEASE_LTO=off
mkdir -p ~/bin
# Test binaries (release, lua,tls) so the test runs only run.
CARGO_TARGET_DIR=~/tgt-full cargo test --release --features lua,tls --no-run \
  --test expiry_propagation --test expired_propagation --test expired_overwrite --test expire_bounds \
  --test eviction --test maxmemory --test field_expiry_ms --test field_expiry_propagation --test hash_field_expiry \
  --test replication --test binary_keys --test backup --lib > ~/build-tests.log 2>&1 && echo built tests
wait
echo SETUP-DONE
