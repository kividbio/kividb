#!/usr/bin/env bash
# base.sh <role>: set-up on every VM. roles: fo (server), foc (client), pf (power-off server), pfc (power-off client),
# bld (cargo build + probes), sm/sr (replica-aof servers), sc (replica-aof load)
set -u
export DEBIAN_FRONTEND=noninteractive
R=$1
exec > "$HOME/base.log" 2>&1
sudo apt-get update -qq
P="redis-tools jq python3-venv curl iproute2 gdb sysstat"
case $R in foc) P="$P docker.io" ;; bld) P="$P build-essential cmake clang pkg-config git libssl-dev" ;; esac
sudo apt-get install -y -qq $P >/dev/null 2>&1 || sudo apt-get install -y -qq $P
curl -sL https://github.com/oras-project/oras/releases/download/v1.2.2/oras_1.2.2_linux_amd64.tar.gz | sudo tar xz -C /usr/local/bin oras
case $R in foc|pfc|sc|bld) python3 -m venv ~/venv && ~/venv/bin/pip install -q redis==5.2.1 hiredis==3.0.0 matplotlib ;; esac
case $R in sc) curl -sL https://go.dev/dl/go1.24.9.linux-amd64.tar.gz | sudo tar xz -C /usr/local ;; esac
sudo mkdir -p /opt/kv /opt/kvrc4 /opt/kvrc6 /opt/redis /data; sudo chown azureuser /opt/kv /opt/kvrc4 /opt/kvrc6 /opt/redis /data
case $R in sm|sr) sudo chown azureuser /mnt ;; esac
echo '* soft nofile 65535' | sudo tee -a /etc/security/limits.conf >/dev/null
echo '* hard nofile 65535' | sudo tee -a /etc/security/limits.conf >/dev/null
mkdir -p ~/b && tar xzf ~/b.tgz -C ~ && ln -sfn ~/b/failover ~/fo
inst() { # tag dest
  local d; d=$(mktemp -d); ( cd "$d" && oras pull "ghcr.io/kividbio/kividb-binaries:$1" >/dev/null && \
    sha256sum -c --ignore-missing SHA256SUMS.txt > ~/sha-$1.txt && tar xzf kividb-linux-x86_64.tar.gz && \
    install -m 755 kividb/kividb "$2/kividb" && "$2/kividb" --version ); rm -rf "$d"
}
inst v1.2.0-rc7 /opt/kv
case $R in bld|sm|sr) inst v1.2.0-rc4 /opt/kvrc4 ;; esac
case $R in bld) inst v1.2.0-rc6 /opt/kvrc6 ;; esac
case $R in fo|pf) cp ~/fo/harness/node.sh ~/node.sh ;; esac
echo BASE-OK $R
