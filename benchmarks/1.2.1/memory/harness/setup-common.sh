#!/bin/bash
# Both VMs. Mirrors the warefeats rig's EC2 user data (infra/lib/kv-bench-stack.ts in warefeats/kv-stores),
# minus awscli, plus python3 and sysstat for this suite's own scripts.
set -euxo pipefail
export DEBIAN_FRONTEND=noninteractive
while fuser /var/lib/dpkg/lock-frontend /var/lib/apt/lists/lock >/dev/null 2>&1; do sleep 5; done
apt-get update -y
apt-get install -y docker.io docker-compose-v2 docker-buildx jq curl git unzip redis-tools python3 sysstat
systemctl enable docker && systemctl start docker
usermod -aG docker azureuser
curl -fsSL https://bun.sh/install | BUN_INSTALL=/usr/local bash
rm -rf /opt/bench
git clone https://github.com/warefeats/kv-stores.git /opt/bench
cd /opt/bench && git checkout a324d29 && /usr/local/bin/bun install --frozen-lockfile
chown -R azureuser:azureuser /opt/bench
docker --version; docker compose version; /usr/local/bin/bun --version
touch /var/tmp/common-ready
