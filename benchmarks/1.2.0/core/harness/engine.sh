#!/usr/bin/env bash
# Start or stop exactly one engine on the server, persistence off, on the
# whole machine, and log the exact command line and version.
#
#   engine.sh start <engine> [port]     engine.sh stop
#
# Engines (binaries from setup-server.sh / fetch-kividb.sh):
#   kividb-rc4     ~/bin/kividb-rc4      KiviDB 1.2.0-rc4, default variant
#   kividb-113     ~/bin/kividb-1.1.3    KiviDB 1.1.3, default variant
#   kividb-master  ~/bin/kividb-master   dry runs only
#   redis          redis-server --io-threads $IO_THREADS (default 8)
#   valkey         valkey-server --io-threads $IO_THREADS (default 8)
#   redis-cluster  $SHARDS (default: one per vCPU) single-threaded primaries, no replicas
#   dragonfly      Dragonfly defaults (one proactor thread per vCPU)
#   garnet         Garnet defaults, log memory and hash index sized for the dataset
#
# Thread counts are each engine's own default for the machine unless a
# variable above says otherwise; launch.log records what was run.
set -euo pipefail

ACTION=${1:?usage: engine.sh start <engine> [port] | stop}
BIN=$HOME/bin
RUN=$HOME/run
LOGS=$HOME/logs
mkdir -p "$LOGS"
NAMES=(kividb-rc4 kividb-1.1.3 kividb-master redis-server dragonfly valkey-server GarnetServer)

stop() {
  for n in "${NAMES[@]}"; do pkill -KILL -x "$n" 2>/dev/null || true; done
  pkill -KILL -f "[d]otnet.*Garnet" 2>/dev/null || true
  for _ in $(seq 240); do
    alive=0
    for n in "${NAMES[@]}"; do pgrep -x "$n" > /dev/null && alive=1; done
    ss -ltn | grep -Eq ':(6379|70[0-9][0-9]) ' && alive=1
    [ $alive = 0 ] && return 0
    sleep 0.5
  done
  echo "engine.sh: something is still running or listening" >&2
  ss -ltnp >&2
  exit 1
}

if [ "$ACTION" = stop ]; then stop; echo "stopped"; exit 0; fi

ENGINE=${2:?engine}
PORT=${3:-6379}
VCPUS=$(nproc)
IO_THREADS=${IO_THREADS:-8}
SHARDS=${SHARDS:-$VCPUS}
stop
rm -rf "$RUN"; mkdir -p "$RUN"; cd "$RUN"
ulimit -n 1000000

record() { printf '%s  %s  [%s]\n    %s\n' "$(date -u +%FT%TZ)" "$ENGINE" "$1" "${*:2}" | tee -a "$LOGS/launch.log"; }
wait_for() {
  for _ in $(seq 240); do
    "$BIN/redis-cli" -p "$1" ping 2>/dev/null | grep -q PONG && return 0
    sleep 0.5
  done
  echo "engine.sh: $ENGINE not answering on $1" >&2; tail -20 "$LOGS/$ENGINE.log" >&2; exit 1
}
launch() { nohup "$@" > "$LOGS/$ENGINE.log" 2>&1 & }

case $ENGINE in
  kividb-rc4|kividb-113|kividb-master)
    b=$BIN/kividb-${ENGINE#kividb-}; [ "$ENGINE" = kividb-113 ] && b=$BIN/kividb-1.1.3
    cmd=("$b" --port "$PORT" --bind 0.0.0.0)
    record "$("$b" --version 2>&1 | head -1)" "${cmd[@]}"
    launch "${cmd[@]}"; wait_for "$PORT"
    grep -i -m1 "I/O model\|io model\|io_uring" "$LOGS/$ENGINE.log" || true ;;
  redis)
    cmd=("$BIN/redis-server" --port "$PORT" --bind 0.0.0.0 --protected-mode no
      --save "" --appendonly no --io-threads "$IO_THREADS")
    record "$("$BIN/redis-server" --version)" "${cmd[@]}"
    launch "${cmd[@]}"; wait_for "$PORT" ;;
  valkey)
    cmd=("$BIN/valkey-server" --port "$PORT" --bind 0.0.0.0 --protected-mode no
      --save "" --appendonly no --io-threads "$IO_THREADS")
    record "$("$BIN/valkey-server" --version)" "${cmd[@]}"
    launch "${cmd[@]}"; wait_for "$PORT" ;;
  dragonfly)
    cmd=("$BIN/dragonfly" --port "$PORT" --bind 0.0.0.0 --dbfilename "" --logtostderr)
    record "$("$BIN/dragonfly" --version 2>&1 | head -1 | sed 's/\x1b\[[0-9;]*m//g')" "${cmd[@]}"
    launch "${cmd[@]}"; wait_for "$PORT" ;;
  garnet)
    # Default log memory (16 GB) is too small for 10M x 2 KB without a
    # storage tier, so it is raised; the hash index is sized for 10M keys.
    export DOTNET_ROOT=$HOME/dotnet
    cmd=("$HOME/garnet/GarnetServer" --port "$PORT" --bind 0.0.0.0 --index 1g --memory 64g)
    record "Garnet $(cat "$HOME/garnet/VERSION")" "${cmd[@]}"
    launch "${cmd[@]}"; wait_for "$PORT" ;;
  redis-cluster)
    ip=$(hostname -I | awk '{print $1}')
    nodes=()
    for i in $(seq 0 $((SHARDS - 1))); do
      p=$((7000 + i)); mkdir -p "$RUN/$p"
      cmd=("$BIN/redis-server" --port "$p" --bind 0.0.0.0 --protected-mode no --save "" --appendonly no
        --cluster-enabled yes --cluster-config-file "nodes-$p.conf" --cluster-node-timeout 15000 --dir "$RUN/$p")
      [ "$i" = 0 ] && record "$("$BIN/redis-server" --version) x $SHARDS primaries, ports 7000-$((7000 + SHARDS - 1))" "${cmd[@]}"
      nohup "${cmd[@]}" > "$LOGS/cluster-$p.log" 2>&1 &
      nodes+=("$ip:$p")
    done
    for i in $(seq 0 $((SHARDS - 1))); do wait_for $((7000 + i)); done
    "$BIN/redis-cli" --cluster create "${nodes[@]}" --cluster-replicas 0 --cluster-yes > "$LOGS/cluster-create.log" 2>&1
    for _ in $(seq 240); do
      "$BIN/redis-cli" -p 7000 cluster info | grep -q cluster_state:ok && break; sleep 0.5
    done
    "$BIN/redis-cli" -p 7000 cluster info | grep -E "cluster_state|cluster_slots_ok" ;;
  *) echo "unknown engine $ENGINE" >&2; exit 2 ;;
esac
echo "up: $ENGINE"
