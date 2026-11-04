#!/usr/bin/env bash
# bld_prep.sh: on the build VM after base.sh: Rust (latest stable), a clone of kividbio/kividb with the rc tags,
# then bld.sh once the Redis binary is in place.
set -u
exec > ~/prep.log 2>&1
until grep -q BASE-OK ~/base.log 2>/dev/null; do sleep 5; done
curl -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain stable > /dev/null
. ~/.cargo/env; rustc --version
git clone -q https://github.com/kividbio/kividb ~/kividb && cd ~/kividb && git fetch -q --tags origin
git rev-parse v1.2.0-rc7^{commit} v1.2.0-rc4^{commit}
for _ in $(seq 1 360); do [ -x /opt/redis/redis-server ] && break; sleep 5; done
/opt/redis/redis-server --version
nohup bash ~/h/bld.sh > ~/bld.log 2>&1 < /dev/null &
echo PREP-DONE
