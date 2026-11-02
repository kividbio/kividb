#!/bin/bash
# Server-side engine control for the memory suite. Every engine runs in Docker with host networking on
# port 6379, data directory on the local NVMe disk (/mnt/nvme/data), one engine at a time.
#
#   eng.sh start <engine> <profile>    engine:  kividb-rc4 | kividb-1.1.3 | redis | valkey | dragonfly
#                                      profile: plain   (no persistence)
#                                               persist (AOF on where the engine has one, automatic rewrites off)
#   eng.sh stop                        stop and remove the engine, wipe its data directory
#   eng.sh pid                         host PID of the engine's main process
#   eng.sh cgroup                      the engine container's cgroup directory
#
# Versions: images are pinned below; KIVIDB_RC4_IMAGE (file /var/tmp/kividb-rc4-image) names the build under test.
set -euo pipefail
NAME=memeng
DATA=/mnt/nvme/data
REDIS_IMG=redis:8.10.3
VALKEY_IMG=valkey/valkey:9.1.3
DF_IMG=docker.dragonflydb.io/dragonflydb/dragonfly:v2.0.2
K113_IMG=kividb/kividb:v1.1.3
RC4_IMG=$(cat /var/tmp/kividb-rc4-image 2>/dev/null || echo kividb/kividb:v1.2.0-rc4)
NCPU=$(nproc)

start() {
  local E=$1 P=$2 IMG ARGS
  sudo mkdir -p $DATA && sudo chmod 777 $DATA
  case $E in
    kividb-rc4|kividb-1.1.3)
      [ "$E" = kividb-rc4 ] && IMG=$RC4_IMG || IMG=$K113_IMG
      ARGS="--port 6379 --bind 0.0.0.0 --protected-mode no --dir /data"
      [ "$P" = persist ] && ARGS="$ARGS --aof --aof-rewrite-percentage 0"
      ;;
    redis|valkey)
      [ "$E" = redis ] && IMG="$REDIS_IMG redis-server" || IMG="$VALKEY_IMG valkey-server"
      # io-threads 8: the upstream recommendation for a many-core box (more than 8 rarely helps)
      ARGS="--port 6379 --bind 0.0.0.0 --protected-mode no --io-threads 8 --dir /data --save ''"
      if [ "$P" = persist ]; then ARGS="$ARGS --appendonly yes --auto-aof-rewrite-percentage 0"; else ARGS="$ARGS --appendonly no"; fi
      ;;
    dragonfly)
      IMG="$DF_IMG dragonfly"
      ARGS="--logtostderr --port 6379 --proactor_threads $NCPU --dir /data"
      # Dragonfly has no AOF; persist = snapshots on BGSAVE only (its default .dfs format)
      if [ "$P" = persist ]; then ARGS="$ARGS --dbfilename dump"; else ARGS="$ARGS --dbfilename ''"; fi
      ;;
    *) echo "unknown engine $E" >&2; exit 2 ;;
  esac
  # shellcheck disable=SC2086
  eval sudo docker run -d --name $NAME --network host --security-opt seccomp=unconfined \
    --ulimit memlock=-1 --ulimit nofile=200000:200000 -v $DATA:/data $IMG $ARGS > /dev/null
  for _ in $(seq 1 120); do redis-cli -p 6379 ping 2>/dev/null | grep -q PONG && break; sleep 0.5; done
  redis-cli -p 6379 ping | grep -q PONG
  echo "$E $P $(sudo docker inspect -f '{{.Config.Image}}' $NAME) $(redis-cli -p 6379 info server | grep -E '^(redis_version|kividb_version|dragonfly_version|valkey_version):' | tr -d '\r' | tr '\n' ' ')"
  sudo docker inspect -f '{{.Config.Cmd}}' $NAME
}
stop() {
  sudo docker logs $NAME > /var/tmp/mem/last-engine.log 2>&1 || true
  sudo docker rm -f $NAME > /dev/null 2>&1 || true
  sudo rm -rf ${DATA:?}/* 2>/dev/null || true
  sync; echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null
}
mkdir -p /var/tmp/mem
case $1 in
  start) start "$2" "$3" ;;
  stop) stop ;;
  pid) sudo docker inspect -f '{{.State.Pid}}' $NAME ;;
  cgroup) echo "/sys/fs/cgroup/system.slice/docker-$(sudo docker inspect -f '{{.Id}}' $NAME).scope" ;;
  *) echo "usage: eng.sh start|stop|pid|cgroup" >&2; exit 2 ;;
esac
