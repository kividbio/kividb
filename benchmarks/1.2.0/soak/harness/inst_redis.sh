#!/usr/bin/env bash
# inst_redis.sh: the Redis 8.10.3 server binary from the official image into /opt/redis (Docker on the Azure VM only)
set -eu
export DEBIAN_FRONTEND=noninteractive
command -v docker >/dev/null || { sudo apt-get install -y -qq docker.io > /dev/null; }
sudo docker pull -q redis:8.10.3 > /dev/null
id=$(sudo docker create redis:8.10.3)
sudo mkdir -p /opt/redis; sudo docker cp $id:/usr/local/bin/redis-server /opt/redis/redis-server; sudo docker rm $id > /dev/null
sudo chown -R azureuser /opt/redis
/opt/redis/redis-server --version
