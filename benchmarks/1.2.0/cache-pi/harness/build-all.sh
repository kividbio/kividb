#!/bin/bash
# build-all.sh   (runs on the builder VM under nohup) -- every engine, in the order the queues need
# them. The 1.2.0 candidate and v1.2.0-rc4 build side by side on this VM with the same toolchain.
until grep -q SETUP-DONE ~/setup.log; do sleep 10; done
bash ~/build.sh kividb-tar v1.1.3 kividb-1.1.3 > ~/build-kividb-1.1.3.log 2>&1
bash ~/build.sh redis 8.10.3 redis-8.10.3 > ~/build-redis-8.10.3.log 2>&1
# install the toolchain rust-toolchain.toml asks for once, so the two builds do not race to install it
. ~/.cargo/env; rustup toolchain install stable --profile minimal -c rustfmt -c clippy > ~/build-toolchain.log 2>&1
bash ~/build.sh kividb 75482abbb258bafe84c0e9d05fc1c904abb19cfd kividb-75482ab > ~/build-kividb-75482ab.log 2>&1 &
bash ~/build.sh kividb v1.2.0-rc4 kividb-rc4 > ~/build-kividb-rc4.log 2>&1 &
bash ~/build.sh redis 6bf6224c3dad518329ddc893ef9c5d58dcbabdeb redis-6bf6224 > ~/build-redis-6bf6224.log 2>&1
wait
echo ALL-BUILT
