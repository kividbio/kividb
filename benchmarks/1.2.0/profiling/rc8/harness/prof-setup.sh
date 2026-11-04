#!/usr/bin/env bash
set -eux
export DEBIAN_FRONTEND=noninteractive
sudo apt-get update -qq
sudo apt-get install -y -qq docker.io git python3 > /dev/null
sudo usermod -aG docker azureuser
sudo ln -sf /usr/bin/python3 /usr/local/bin/python
rm -rf ~/kividb
git clone -q https://github.com/kividbio/kividb ~/kividb
cd ~/kividb
git fetch -q --tags origin
git checkout -q 082dab3d8929c180bccf993835e28992ea054fb5
git log --oneline -1
git diff --stat v1.2.0-rc7 082dab3d8929c180bccf993835e28992ea054fb5 -- scripts/perf || true
bash ~/p/callgrind.sh rc7:v1.2.0-rc7 rc8:082dab3d8929c180bccf993835e28992ea054fb5
