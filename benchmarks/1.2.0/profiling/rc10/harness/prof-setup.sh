#!/usr/bin/env bash
# rc10 round of the profiling suite, VM side: Docker, a clone of the public repo,
# then callgrind-keep.sh for rc8 and rc10 (one run each, rc8 first).
set -eux
export DEBIAN_FRONTEND=noninteractive
sudo apt-get update -qq
sudo apt-get install -y -qq docker.io git python3 valgrind > /dev/null
sudo usermod -aG docker azureuser
sudo ln -sf /usr/bin/python3 /usr/local/bin/python
rm -rf ~/kividb
git clone -q https://github.com/kividbio/kividb ~/kividb
cd ~/kividb
git fetch -q --tags origin
git rev-parse 'v1.2.0-rc10^{commit}'
git checkout -q 39bfacd2
git log --oneline -1
git diff --stat 082dab3d 39bfacd2 -- scripts/perf || true
bash ~/p/callgrind-keep.sh rc8:082dab3d rc10:39bfacd2
