#!/usr/bin/env bash
# server.sh start-master | start-replica <master-ip> | stop      (env ENGINE=kividb|before, EXTRA="directive;directive")
# The rc7 soak's server.sh for these VMs: port 6379, data on the local NVMe disk (/mnt/kv), AOF on, everysec,
# sampler.sh writing samples-<role>.csv every 2 s. ENGINE=kividb is the build under test (rc9 here), rc8 is 082dab3d.
set -u
D=/mnt/kv
ENGINE=${ENGINE:-kividb}
case $1 in
start-master|start-replica)
  pkill -f '[s]ampler.sh'; pkill -9 -f '[/]opt/kv/kividb'
  for _ in $(seq 1 120); do pgrep -f '[/]opt/kv/kividb' >/dev/null || break; sleep 0.5; done
  for _ in $(seq 1 120); do ss -ltn | grep -q ':6379 ' || break; sleep 0.5; done
  sudo rm -rf "$D"; sudo mkdir -p "$D"; sudo chown azureuser "$D"
  {
    echo "port 6379"; echo "bind 0.0.0.0"; echo "protected-mode no"; echo "dir $D"
    echo "appendonly yes"; echo "appendfsync everysec"; echo "update-check no"
    [ "$1" = start-replica ] && echo "replicaof $2 6379"
    echo "${EXTRA:-}" | tr ';' '\n' | grep -v '^ *$'
  } > "$D/node.conf"
  case $ENGINE in
    before) cmd=(/opt/kv/kividb-before --configfile "$D/node.conf") ;;
    final)  cmd=(/opt/kv/kividb-final --configfile "$D/node.conf") ;;
    final2) cmd=(/opt/kv/kividb-final2 --configfile "$D/node.conf") ;;
    rc8)    cmd=(/opt/kv/kividb-rc8 --configfile "$D/node.conf") ;;   # rc9 retest: 082dab3d
    *)      cmd=(/opt/kv/kividb --configfile "$D/node.conf") ;;
  esac
  echo "${cmd[*]}" > "$D/cmd"
  (cd "$D" && exec nohup "${cmd[@]}" >> "$D/server.log" 2>&1 < /dev/null) &
  echo $! > "$D/pid"
  for _ in $(seq 1 100); do redis-cli -p 6379 PING 2>/dev/null | grep -q PONG && break; sleep 0.2; done
  redis-cli -p 6379 PING | grep -q PONG || { echo "server did not start"; tail -5 "$D/server.log"; exit 1; }
  role=$([ "$1" = start-master ] && echo master || echo replica)
  nohup bash "$HOME/sampler.sh" 6379 "$D/pid" "$HOME/samples-$role.csv" 2 > /dev/null 2>&1 < /dev/null &
  echo "started $ENGINE $role pid $(cat "$D/pid")"
  ;;
stop) pkill -f '[s]ampler.sh'; redis-cli -p 6379 SHUTDOWN NOSAVE >/dev/null 2>&1; sleep 1; pkill -9 -f '[/]opt/kv/kividb' ;;
esac
