#!/usr/bin/env bash
# 1.2.1 round of the profiling suite, VM side: Docker, a clone of the public
# repo, then callgrind-keep.sh for 1.2.0 (tag v1.2.0) and 1.2.1 (878746e4).
# The harness files are in ~/p.
set -eux
export DEBIAN_FRONTEND=noninteractive
sudo apt-get update -qq
sudo apt-get install -y -qq docker.io git python3 python3-matplotlib > /dev/null
sudo ln -sf /usr/bin/python3 /usr/local/bin/python
rm -rf ~/kividb
git clone -q https://github.com/kividbio/kividb ~/kividb
cd ~/kividb
git fetch -q --tags origin
git rev-parse 'v1.2.0^{commit}'
git checkout -q 878746e41e2f7a9050852e6ec5ef67394778368c
git log --oneline -1
# the perf scripts are the same in both trees
git diff --stat v1.2.0 878746e4 -- scripts/perf
# the 1.2.1 tree handed to the release checks is this commit's tree
if [ -f ~/src-878746e4.tgz ]; then
  rm -rf ~/tgz && mkdir ~/tgz && tar -xzf ~/src-878746e4.tgz -C ~/tgz
  rm -rf ~/arch && mkdir ~/arch && git archive 878746e4 | tar -xf - -C ~/arch
  if diff -r ~/tgz ~/arch > ~/tgz.diff; then echo "tgz == git archive 878746e4"; else echo "tgz differs, see ~/tgz.diff"; fi
fi
bash ~/p/callgrind-keep.sh v120:v1.2.0 v121:878746e4
