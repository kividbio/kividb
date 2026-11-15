#!/usr/bin/env bash
# vm_extra.sh: Redis 8.10.2 test tree (as CI clones it) and the 1.2.0 release full binary (SHA256SUMS checked).
set -ux
[ -d ~/redis-tests ] || git clone -q --depth 1 --branch 8.10.2 https://github.com/redis/redis.git ~/redis-tests
mkdir -p ~/r120 && cd ~/r120
curl -sSLO https://github.com/kividbio/kividb/releases/download/v1.2.0/kividb-full-linux-x86_64.tar.gz
curl -sSLO https://github.com/kividbio/kividb/releases/download/v1.2.0/SHA256SUMS.txt
grep ' kividb-full-linux-x86_64.tar.gz$' SHA256SUMS.txt | sha256sum -c - && echo SHA-OK
mkdir -p full && tar xzf kividb-full-linux-x86_64.tar.gz -C full && find full -type f -name kividb
