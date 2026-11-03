#!/usr/bin/env bash
# Sourced on the server VM. Starts and stops one engine at a time on all of
# the VM's CPUs, no persistence, each configured the way its own docs
# recommend for a multi-core box.
#   up ENGINE PORT      down ENGINE PORT      rss_kb ENGINE PORT
# Engines:
#   kividb-rc4, kividb-1.1.3   ~/bin/kividb-<ver>, defaults (worker threads = vCPUs - 2)
#   redis                      Redis 8.10.3 from source, io-threads $REDIS_IO_THREADS (8)
#   valkey                     valkey/valkey:9.1.3 (host network), io-threads $VALKEY_IO_THREADS (6)
#   dragonfly                  dragonfly v2.0.2 (host network), proactor_threads = vCPUs
#   redis-cluster              $SHARDS Redis 8.10.3 primaries, one per vCPU, each pinned, no replicas
#   kvrocks                    apache/kvrocks:2.17.0 (host network), workers = vCPUs, data on the local NVMe
set -u
NCPU=$(nproc)
REDIS_IO_THREADS=${REDIS_IO_THREADS:-${IO_THREADS:-8}}   # best of 4/6/8/12 in iothreads-sweep.sh
VALKEY_IO_THREADS=${VALKEY_IO_THREADS:-${IO_THREADS:-6}} # best of 4/6/8/12 in iothreads-sweep.sh
SHARDS=${SHARDS:-$NCPU}
VALKEY_IMG=${VALKEY_IMG:-valkey/valkey:9.1.3}
DF_IMG=${DF_IMG:-docker.dragonflydb.io/dragonflydb/dragonfly:v2.0.2}
KVROCKS_IMG=${KVROCKS_IMG:-apache/kvrocks:2.17.0}
KVROCKS_DIR=${KVROCKS_DIR:-/mnt/kvrocks}

wait_ping() { # port
  for _ in $(seq 300); do
    redis-cli -p "$1" ping 2> /dev/null | grep -q PONG && return 0
    sleep 0.1
  done
  return 1
}

up() { # engine port
  local e=$1 p=$2 d=/tmp/srv-$2
  rm -rf "$d"; mkdir -p "$d"
  case $e in
    kividb-*)
      (cd "$d" && setsid -f ~/bin/"$e" --port "$p" --bind 0.0.0.0 --protected-mode no --dir "$d" \
        --loglevel warning > "$d/log" 2>&1 < /dev/null)
      wait_ping "$p" || { echo "up $e failed"; tail -5 "$d/log"; return 1; }
      redis-cli -p "$p" config set save "" > /dev/null ;;
    redis)
      setsid -f ~/bin/redis-server --port "$p" --bind 0.0.0.0 --protected-mode no --dir "$d" \
        --save "" --appendonly no --io-threads "$REDIS_IO_THREADS" > "$d/log" 2>&1 < /dev/null
      wait_ping "$p" || { echo "up redis failed"; tail -5 "$d/log"; return 1; } ;;
    valkey)
      docker rm -f "eng$p" > /dev/null 2>&1
      docker run -d --name "eng$p" --network host "$VALKEY_IMG" valkey-server --port "$p" --bind 0.0.0.0 \
        --protected-mode no --save "" --appendonly no --io-threads "$VALKEY_IO_THREADS" > /dev/null
      wait_ping "$p" || { echo "up valkey failed"; docker logs "eng$p" | tail -5; return 1; } ;;
    dragonfly)
      docker rm -f "eng$p" > /dev/null 2>&1
      docker run -d --name "eng$p" --network host --ulimit memlock=-1 "$DF_IMG" \
        --port "$p" --proactor_threads "$NCPU" --maxmemory 48gb --dbfilename "" > /dev/null
      wait_ping "$p" || { echo "up dragonfly failed"; docker logs "eng$p" | tail -5; return 1; } ;;
    kvrocks)
      docker rm -f "eng$p" > /dev/null 2>&1
      sudo rm -rf "$KVROCKS_DIR/$p"; mkdir -p "$KVROCKS_DIR/$p"; chmod 777 "$KVROCKS_DIR/$p"
      # the image's own kvrocks.conf (defaults: 4 GB block cache, snappy) plus these flags
      docker run -d --name "eng$p" --network host -v "$KVROCKS_DIR/$p:/data" "$KVROCKS_IMG" \
        --bind 0.0.0.0 --port "$p" --workers "$NCPU" --dir /data > /dev/null
      wait_ping "$p" || { echo "up kvrocks failed"; docker logs "eng$p" | tail -5; return 1; } ;;
    redis-cluster)
      local i nodes=""
      for i in $(seq 0 $((SHARDS - 1))); do
        local q=$((p + i)) dd=$d/$i
        mkdir -p "$dd"
        (cd "$dd" && setsid -f taskset -c "$i" ~/bin/redis-server --port "$q" --bind 0.0.0.0 --protected-mode no \
          --dir "$dd" --save "" --appendonly no --cluster-enabled yes --cluster-config-file nodes.conf \
          > "$dd/log" 2>&1 < /dev/null)
        nodes="$nodes $(hostname -I | awk '{print $1}'):$q"
      done
      for i in $(seq 0 $((SHARDS - 1))); do wait_ping $((p + i)) || { echo "shard $i failed"; return 1; }; done
      # shellcheck disable=SC2086
      redis-cli --cluster create $nodes --cluster-replicas 0 --cluster-yes > "$d/create.log" 2>&1
      for _ in $(seq 100); do
        redis-cli -p "$p" cluster info | grep -q 'cluster_state:ok' && break
        sleep 0.2
      done ;;
    *) echo "unknown engine $e"; return 1 ;;
  esac
}

down() { # engine port
  local e=$1 p=$2
  case $e in
    valkey | dragonfly | kvrocks) docker rm -f "eng$p" > /dev/null 2>&1 ;;
    redis-cluster)
      for i in $(seq 0 $((SHARDS - 1))); do redis-cli -p $((p + i)) shutdown nosave > /dev/null 2>&1; done ;;
    *) redis-cli -p "$p" shutdown nosave > /dev/null 2>&1 ;;
  esac
  for _ in $(seq 100); do redis-cli -p "$p" ping > /dev/null 2>&1 || break; sleep 0.2; done
  pkill -f -- "--port $p " 2> /dev/null
  [ "$e" = kvrocks ] && sudo rm -rf "${KVROCKS_DIR:?}/$p"
  sleep 1
}

pid_info() { redis-cli -p "$1" info server | tr -d '\r' | awk -F: '/^process_id:/ {print $2}'; }

pids_of() { # engine port: every server process
  case $1 in
    valkey | dragonfly | kvrocks) # the container's first process (tini in some images) and its children
      local c; c=$(docker inspect -f '{{.State.Pid}}' "eng$2"); echo "$c"; pgrep -P "$c" ;;
    redis-cluster) for i in $(seq 0 $((SHARDS - 1))); do pid_info $(($2 + i)); done ;;
    *) pid_info "$2" ;;
  esac
}

rss_kb() { # engine port: summed resident set size, KiB
  local t=0 pid
  for pid in $(pids_of "$1" "$2"); do
    t=$((t + $(awk '/VmRSS/ {print $2}' "/proc/$pid/status" 2> /dev/null || echo 0)))
  done
  echo "$t"
}

version_of() { # port
  redis-cli -p "$1" info server | tr -d '\r' | grep -E '^(redis_version|kividb_version|dragonfly_version|valkey_version|kvrocks_version|server_name|version):' | tr '\n' ' '
}
