#!/usr/bin/env bash
# cnode.sh: one cluster node on this host (the rc7 soak's node.sh, for these VMs).
#   cnode.sh start <kividb|before> <port>     cluster-enabled, node timeout 5000, AOF on, data in /data/<port>
#   cnode.sh kill9|restart|resync <port>      resync: kill -9, delete the data but keep nodes.conf, start again
#   cnode.sh wipeall
set -u
cmd=$1; port=${2:-}
D=/data/$port
launch() {
  (cd "$D" && exec nohup $(cat "$D/bin") "$D/node.conf" >> "$D/server.log" 2>&1 < /dev/null) &
  echo $! > "$D/pid"
  for _ in $(seq 1 100); do redis-cli -p "$port" PING 2>/dev/null | grep -q -E 'PONG|LOADING' && break; sleep 0.1; done
}
case $cmd in
start)
  engine=$2; port=$3; D=/data/$port
  mkdir -p "$D"
  printf 'port %s\nbind 0.0.0.0\nprotected-mode no\ndir %s\nappendonly yes\nupdate-check no\ncluster-enabled yes\ncluster-node-timeout 5000\ncluster-config-file nodes.conf\n' "$port" "$D" > "$D/node.conf"
  case $engine in
    before) echo "/opt/kv/kividb-before --configfile" > "$D/bin" ;;
    final)  echo "/opt/kv/kividb-final --configfile" > "$D/bin" ;;
    *)      echo "/opt/kv/kividb --configfile" > "$D/bin" ;;
  esac
  launch; echo "started $engine :$port pid $(cat "$D/pid")" ;;
restart) for _ in $(seq 1 100); do ss -ltn | grep -q ":$port " || break; sleep 0.05; done; launch; echo "restarted :$port pid $(cat "$D/pid")" ;;
kill9) kill -9 "$(cat "$D/pid")" ;;
resync)
  p=$(cat "$D/pid"); kill -9 "$p"
  for _ in $(seq 1 100); do kill -0 "$p" 2>/dev/null || break; sleep 0.05; done
  for _ in $(seq 1 100); do ss -ltn | grep -q ":$port " || break; sleep 0.05; done
  find "$D" -maxdepth 1 -type f ! -name nodes.conf ! -name node.conf ! -name bin ! -name server.log ! -name pid -delete
  rm -rf "$D/appendonlydir"
  echo "=== resync $(date -u +%FT%T.%3NZ)" >> "$D/server.log"
  launch; echo "resync :$port pid $(cat "$D/pid")" ;;
wipeall) pkill -9 -f '[/]opt/kv/kividb'; sleep 0.5; sudo rm -rf /data; sudo mkdir -p /data; sudo chown azureuser /data ;;
esac
