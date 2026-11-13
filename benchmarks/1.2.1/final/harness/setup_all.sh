#!/usr/bin/env bash
# setup_all.sh <role: srv|rep|cl>  (on each VM, once). Packages, /data disk, venv, 1.2.0 release binaries
# (SHA256SUMS checked), memtier 2.5.2 (rep, cl), Redis 8.10.2 test tree (rep), Docker (srv), Go (cl).
set -u
ROLE=$1
exec > ~/setup.log 2>&1
export DEBIAN_FRONTEND=noninteractive
sudo apt-get update -qq
PK="build-essential cmake clang pkg-config libssl-dev git curl jq sysstat python3-venv tcl tcllib redis-tools numactl autoconf automake libpcre3-dev libevent-dev zlib1g-dev"
[ $ROLE != cl ] && PK="$PK docker.io bpftrace"
[ $ROLE = cl ] && PK="$PK golang-go"
sudo apt-get install -y -qq $PK > /dev/null
sudo systemctl disable --now redis-server > /dev/null 2>&1 || true
echo '* soft nofile 1000000
* hard nofile 1000000' | sudo tee /etc/security/limits.d/bench.conf > /dev/null
sudo sysctl -qw net.core.somaxconn=65535 net.ipv4.tcp_max_syn_backlog=65535 net.ipv4.ip_local_port_range="1024 65000"
if [ $ROLE != cl ]; then
  sudo mkfs.ext4 -q -F /dev/sdb && sudo mkdir -p /data && sudo mount -o noatime /dev/sdb /data && sudo chown azureuser /data
  echo "UUID=$(sudo blkid -s UUID -o value /dev/sdb) /data ext4 noatime,nofail 0 2" | sudo tee -a /etc/fstab > /dev/null
fi
[ $ROLE != cl ] && { sudo systemctl enable --now docker; sudo usermod -aG docker azureuser; }
python3 -m venv ~/venv && ~/venv/bin/pip -q install redis==5.2.1 hiredis==3.0.0 matplotlib && echo venv-ok
mkdir -p ~/bin ~/r120 && cd ~/r120
REPO=https://github.com/kividbio/kividb/releases/download/v1.2.0
curl -fsSLO $REPO/SHA256SUMS.txt
for t in kividb-linux-aarch64 kividb-full-linux-aarch64; do
  curl -fsSLO $REPO/$t.tar.gz
  grep -E " (\./)?$t.tar.gz$" SHA256SUMS.txt | sed 's# \./# #' | sha256sum -c - && echo "SHA-OK $t"
  mkdir -p $t && tar xzf $t.tar.gz -C $t
done
install -m755 "$(find kividb-linux-aarch64 -type f -name kividb | head -1)" ~/bin/kividb-120-default
install -m755 "$(find kividb-full-linux-aarch64 -type f -name kividb | head -1)" ~/bin/kividb-120-full
sha256sum ~/bin/kividb-120-*
if [ $ROLE != srv ]; then
  mkdir -p ~/src && cd ~/src && git clone -q --depth 1 --branch 2.5.2 https://github.com/RedisLabs/memtier_benchmark && cd memtier_benchmark \
   && autoreconf -ivf > /dev/null 2>&1 && ./configure > /dev/null && make -j"$(nproc)" > /dev/null && sudo make install > /dev/null
  memtier_benchmark --version | head -1
fi
[ $ROLE = rep ] && git clone -q --depth 1 --branch 8.10.2 https://github.com/redis/redis.git ~/redis-tests && echo redis-tests-ok
mkdir -p ~/k121 && tar xzf ~/src.tgz -C ~/k121 && cd ~/k121 && git init -q && git add -A && git add -f tests/fixtures tests/e2e/requirements.txt 2>/dev/null; git -c user.email=t@t -c user.name=t commit -qm src && echo tree-ok
echo SETUP-DONE
