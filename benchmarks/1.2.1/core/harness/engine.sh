#!/usr/bin/env bash
# Start or stop exactly one KiviDB build on the server, persistence off, on the
# whole machine, and log the exact command line and version.
# (The 1.2.0 suite's engine.sh, cut down to the three builds of this check.)
#
#   engine.sh start <engine> [port]     engine.sh stop
#
#   kividb-121     ~/bin/kividb-121     KiviDB 1.2.1 (878746e4), release-way build, default variant
#   kividb-120rel  ~/bin/kividb-120rel  KiviDB 1.2.0 release binary (kividb-linux-aarch64)
#   kividb-120src  ~/bin/kividb-120src  KiviDB 1.2.0 (tag v1.2.0), release-way build
set -euo pipefail

ACTION=${1:?usage: engine.sh start <engine> [port] | stop}
BIN=$HOME/bin
RUN=$HOME/run
LOGS=$HOME/logs
mkdir -p "$LOGS"
NAMES=(kividb-121 kividb-120rel kividb-120src)

stop() {
  for n in "${NAMES[@]}"; do pkill -KILL -x "$n" 2>/dev/null || true; done
  for _ in $(seq 240); do
    alive=0
    for n in "${NAMES[@]}"; do pgrep -x "$n" > /dev/null && alive=1; done
    ss -ltn | grep -Eq ':6379 ' && alive=1
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

case $ENGINE in
  kividb-121|kividb-120rel|kividb-120src)
    b=$BIN/$ENGINE
    cmd=("$b" --port "$PORT" --bind 0.0.0.0)
    record "$("$b" --version 2>&1 | head -1) sha256 $(sha256sum "$b" | cut -c1-16)" "${cmd[@]}"
    nohup "${cmd[@]}" > "$LOGS/$ENGINE.log" 2>&1 &
    wait_for "$PORT"
    grep -i -m1 "I/O model\|io model\|io_uring" "$LOGS/$ENGINE.log" || true ;;
  *) echo "unknown engine $ENGINE" >&2; exit 2 ;;
esac
echo "up: $ENGINE"
