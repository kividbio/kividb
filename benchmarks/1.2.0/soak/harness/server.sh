#!/usr/bin/env bash
# server.sh start-master | start-replica <master-ip> | stop      (env ENGINE=kividb|redis, EXTRA="directive;directive")
# Port 6379, data on the local SSD (/mnt/kv), AOF on, appendfsync everysec, sampler.sh writing
# samples-<role>.csv every 10 s. Redis: 8.10.3 binary from the official image, io-threads 4, no RDB saves.
set -u
D=/mnt/kv
ENGINE=${ENGINE:-kividb}
case $1 in
start-master|start-replica)
  pkill -f '[s]ampler.sh'; pkill -9 -f '[/]opt/kv/kividb|[/]opt/kv113/kividb|[/]opt/kvrc3/kividb'; pkill -9 -f '[/]opt/redis/redis-server'
  for _ in $(seq 1 120); do pgrep -f '[/]opt/kv/kividb|[/]opt/kv113/kividb|[/]opt/kvrc3/kividb|[/]opt/redis/redis-server' >/dev/null || break; sleep 0.5; done
  for _ in $(seq 1 120); do ss -ltn | grep -q ':6379 ' || break; sleep 0.5; done
  rm -rf "$D"; mkdir -p "$D"
  {
    echo "port 6379"; echo "bind 0.0.0.0"; echo "protected-mode no"; echo "dir $D"
    echo "appendonly yes"; echo "appendfsync everysec"
    if [ "$ENGINE" = redis ]; then echo "io-threads 4"; echo 'save ""'; else echo "update-check no"; fi
    [ "$1" = start-replica ] && echo "replicaof $2 6379"
    echo "${EXTRA:-}" | tr ';' '\n' | grep -v '^ *$'
  } > "$D/node.conf"
  case $ENGINE in
    redis)     cmd=(/opt/redis/redis-server "$D/node.conf") ;;
    kividb113) cmd=(/opt/kv113/kividb --configfile "$D/node.conf") ;;   # 1.1.3 release binary, for regression rows
    kividbrc3) cmd=(/opt/kvrc3/kividb --configfile "$D/node.conf") ;;   # 1.2.0-rc3 release binary
    *)         cmd=(/opt/kv/kividb --configfile "$D/node.conf") ;;
  esac
  (cd "$D" && exec nohup "${cmd[@]}" >> "$D/server.log" 2>&1 < /dev/null) &
  echo $! > "$D/pid"
  for _ in $(seq 1 100); do redis-cli -p 6379 PING 2>/dev/null | grep -q PONG && break; sleep 0.2; done
  redis-cli -p 6379 PING | grep -q PONG || { echo "server did not start"; tail -5 "$D/server.log"; exit 1; }
  role=$([ "$1" = start-master ] && echo master || echo replica)
  nohup bash "$HOME/sampler.sh" 6379 "$D/pid" "$HOME/samples-$role.csv" 10 > /dev/null 2>&1 < /dev/null &
  echo "started $ENGINE $role pid $(cat "$D/pid")"
  ;;
stop) pkill -f '[s]ampler.sh'; redis-cli -p 6379 SHUTDOWN >/dev/null 2>&1; sleep 2; pkill -9 -f '[/]opt/kv/kividb|[/]opt/kv113/kividb|[/]opt/kvrc3/kividb|[/]opt/redis/redis-server' ;;
esac
