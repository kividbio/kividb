#!/usr/bin/env bash
# Start exactly one engine on this host, with persistence off, and record the
# command it was started with.
#
#   scripts/start-server.sh <kividb|dragonfly|redis|redis-cluster|valkey|keydb|garnet>
#
# Environment:
#   CORES       run the engine on this many CPUs only (taskset), with its
#               thread or shard count set to match: for core-scaling runs.
#               Unset: the whole machine.
#   PORT        client port (default 6379; redis-cluster uses 7000 upwards)
#   THREADS     worker threads for KiviDB and Dragonfly (default: vCPUs - 2,
#               which is also KiviDB's own default; CORES when that is set)
#   IO_THREADS  I/O threads for Redis and Valkey (default 8, at most CORES).
#               Both still execute commands on one thread; redis-cluster is
#               the multi-core comparison.
#   SHARDS      Redis Cluster primaries (default: vCPUs - 2, or CORES), no replicas
#   KEYDB_THREADS  KeyDB server threads (default 16, at most CORES)
#   BIN         where the engine binaries are (default ~/bin)
#
# Every engine is started the same way: an empty working directory, no
# snapshot or append-only file, no memory limit, listening on all addresses.
# The command line and the version are appended to ~/logs/launch.log, so a
# result can always be traced to how the engine was configured.
set -euo pipefail

ENGINE=${1:?usage: start-server.sh <kividb|dragonfly|redis|redis-cluster|valkey|keydb|garnet>}
HERE=$(cd "$(dirname "$0")" && pwd)
PORT=${PORT:-6379}
VCPUS=$(nproc)
CORES=${CORES:-}
if [ -n "$CORES" ]; then
  THREADS=${THREADS:-$CORES}
  SHARDS=${SHARDS:-$CORES}
  pin=(taskset -c "0-$((CORES - 1))")
  limit=$CORES
else
  THREADS=${THREADS:-$((VCPUS - 2))}
  SHARDS=${SHARDS:-$((VCPUS - 2))}
  pin=()
  limit=$VCPUS
fi
at_most() { [ "$1" -lt "$2" ] && echo "$1" || echo "$2"; }
IO_THREADS=$(at_most "${IO_THREADS:-8}" "$limit")
KEYDB_THREADS=$(at_most "${KEYDB_THREADS:-16}" "$limit")
BIN=${BIN:-$HOME/bin}
RUN=$HOME/run
LOGS=$HOME/logs

"$HERE/stop-server.sh"
rm -rf "$RUN"
mkdir -p "$RUN" "$LOGS"
cd "$RUN"
ulimit -n 1000000 2>/dev/null || ulimit -n 65535

record() { # version, command...
  local version=$1
  shift
  printf '%s  %s  [%s]%s\n    %s\n' "$(date -u +%FT%TZ)" "$ENGINE" "$version" \
    "${CORES:+  on $CORES cores}" "$*" | tee -a "$LOGS/launch.log"
}

launch() { # command...
  nohup "${pin[@]}" "$@" > "$LOGS/$ENGINE.log" 2>&1 &
}

wait_for() { # port
  for _ in $(seq 120); do
    redis-cli -p "$1" ping 2>/dev/null | grep -q PONG && return 0
    sleep 0.5
  done
  echo "start-server: $ENGINE did not answer on port $1" >&2
  tail -20 "$LOGS/$ENGINE.log" >&2
  exit 1
}

case $ENGINE in
  kividb)
    cmd=("$BIN/kividb" --port "$PORT" --bind 0.0.0.0 --threads "$THREADS")
    record "$("$BIN/kividb" --version 2>&1 | head -1)" "${pin[@]}" "${cmd[@]}"
    launch "${cmd[@]}"
    wait_for "$PORT"
    # A fallback from io_uring would be a different engine from the one meant.
    grep -i "I/O model" "$LOGS/$ENGINE.log" | tail -1
    ;;
  dragonfly)
    cmd=("$BIN/dragonfly" --port "$PORT" --bind 0.0.0.0 --proactor_threads "$THREADS"
      --dbfilename "" --logtostderr)
    record "$("$BIN/dragonfly" --version 2>&1 | head -1 | sed 's/\x1b\[[0-9;]*m//g')" "${pin[@]}" "${cmd[@]}"
    launch "${cmd[@]}"
    wait_for "$PORT"
    ;;
  redis)
    cmd=(redis-server --port "$PORT" --bind 0.0.0.0 --protected-mode no
      --save "" --appendonly no --io-threads "$IO_THREADS")
    record "$(redis-server --version)" "${pin[@]}" "${cmd[@]}"
    launch "${cmd[@]}"
    wait_for "$PORT"
    ;;
  valkey)
    cmd=("$BIN/valkey-server" --port "$PORT" --bind 0.0.0.0 --protected-mode no
      --save "" --appendonly no --io-threads "$IO_THREADS")
    record "$("$BIN/valkey-server" --version)" "${pin[@]}" "${cmd[@]}"
    launch "${cmd[@]}"
    wait_for "$PORT"
    ;;
  keydb)
    cmd=("$BIN/keydb-server" --port "$PORT" --bind 0.0.0.0 --protected-mode no
      --save "" --appendonly no --server-threads "$KEYDB_THREADS")
    record "$("$BIN/keydb-server" --version)" "${pin[@]}" "${cmd[@]}"
    launch "${cmd[@]}"
    wait_for "$PORT"
    ;;
  garnet)
    # Garnet sizes its own thread pool; its hash index is given room for the
    # 10 million keys of the matrix (the default, 128 MB, is sized for fewer).
    export DOTNET_ROOT=$HOME/dotnet
    cmd=("$HOME/garnet/GarnetServer" --port "$PORT" --bind 0.0.0.0 --index 1g)
    record "Garnet $(cat "$HOME/garnet/VERSION" 2>/dev/null)" "${pin[@]}" "${cmd[@]}"
    launch "${cmd[@]}"
    wait_for "$PORT"
    ;;
  redis-cluster)
    # One single-threaded primary per core, no replicas: Redis using the
    # whole machine, which is the like-for-like for a multi-threaded engine.
    ip=$(hostname -I | awk '{print $1}')
    nodes=()
    for i in $(seq 0 $((SHARDS - 1))); do
      port=$((7000 + i))
      mkdir -p "$RUN/$port"
      cmd=(redis-server --port "$port" --bind 0.0.0.0 --protected-mode no
        --save "" --appendonly no --cluster-enabled yes
        --cluster-config-file "nodes-$port.conf" --cluster-node-timeout 15000
        --dir "$RUN/$port")
      [ "$i" = 0 ] && record "$(redis-server --version) x $SHARDS primaries, ports 7000-$((7000 + SHARDS - 1))" "${pin[@]}" "${cmd[@]}"
      nohup "${pin[@]}" "${cmd[@]}" > "$LOGS/$ENGINE-$port.log" 2>&1 &
      nodes+=("$ip:$port")
    done
    for i in $(seq 0 $((SHARDS - 1))); do wait_for $((7000 + i)); done
    redis-cli --cluster create "${nodes[@]}" --cluster-replicas 0 --cluster-yes > "$LOGS/$ENGINE-create.log" 2>&1
    for _ in $(seq 120); do
      redis-cli -p 7000 cluster info 2>/dev/null | grep -q "cluster_state:ok" && break
      sleep 0.5
    done
    redis-cli -p 7000 cluster info | grep -E "cluster_state|cluster_slots_ok|cluster_known_nodes"
    ;;
  *)
    echo "unknown engine: $ENGINE" >&2
    exit 2
    ;;
esac
echo "start-server: $ENGINE is up"
