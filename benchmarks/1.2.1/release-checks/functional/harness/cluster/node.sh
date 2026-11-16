#!/usr/bin/env bash
# node.sh: start / kill / stop one server process on this host. Runs on each server VM.
#   node.sh start <engine> <port> ["directive value;directive value;..."]   engine: kividb | redis
#   node.sh kill9|term|stop|cont|restart|pid|wipe <port>
#   node.sh wipeall
# Each server gets a config file /data/<port>/node.conf (common directives + the extra ones) and its data
# in /data/<port>. `restart` starts the same server again with the same file.
set -u
cmd=$1; port=${2:-}
D=/data/$port
pidof_port() { cat "$D/pid" 2>/dev/null; }
launch() {
  (cd "$D" && exec nohup $(cat "$D/bin") "$D/node.conf" >> "$D/server.log" 2>&1 < /dev/null) &
  echo $! > "$D/pid"
  for _ in $(seq 1 100); do
    redis-cli -p "$port" PING 2>/dev/null | grep -q -E 'PONG|LOADING' && break
    sleep 0.1
  done
}
case $cmd in
start)
  engine=$2; port=$3; extra=${4:-}; D=/data/$port
  mkdir -p "$D"
  {
    echo "port $port"
    echo "bind 0.0.0.0"
    echo "protected-mode no"
    echo "dir $D"
    echo "appendonly yes"
    [ "$engine" = redis ] && { echo "save \"\""; echo "io-threads 2"; }
    echo "$extra" | tr ';' '\n' | sed 's/^ *//' | grep -v '^$'
  } > "$D/node.conf"
  case $engine in
    kividb) echo "/opt/kv/kividb --configfile" > "$D/bin" ;;
    redis)  echo "/opt/redis/redis-server" > "$D/bin" ;;
  esac
  launch
  echo "started $engine :$port pid $(cat "$D/pid")"
  ;;
restart) launch; echo "restarted :$port pid $(cat "$D/pid")" ;;
kill9) kill -9 "$(pidof_port)" ;;
term)  p=$(pidof_port); kill -TERM "$p"; for _ in $(seq 1 300); do kill -0 "$p" 2>/dev/null || break; sleep 0.1; done ;;
stop)  kill -STOP "$(pidof_port)" ;;
cont)  kill -CONT "$(pidof_port)" ;;
pid)   pidof_port ;;
wipe)  kill -9 "$(pidof_port)" 2>/dev/null; sleep 0.3; rm -rf "$D" ;;
wipeall) pkill -9 -f '/opt/kv/kividb|/opt/redis/redis-server'; sleep 0.5; rm -rf /data/*
         sudo iptables -F INPUT; sudo iptables -F OUTPUT ;;
*) echo "usage"; exit 2 ;;
esac
