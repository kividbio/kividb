#!/bin/bash
# redis-server stand-in that starts KiviDB, for test suites that spawn `redis-server` from PATH.
# KIVIDB_BIN picks the binary. Flags KiviDB has no use for are dropped:
#   --set-proc-title, --save, --daemonize no; --appendonly no/yes maps to (nothing)/--aof;
#   --pidfile is honoured here (this script writes its own pid, then execs KiviDB in place);
#   a positional `*.conf` becomes --configfile.
# `redis-server --version` answers like Redis does, for suites that check it.
BIN="${KIVIDB_BIN:?set KIVIDB_BIN}"
args=()
pidfile=""
if [ "$1" = "--version" ] || [ "$1" = "-v" ]; then
  echo "Redis server v=8.10.0 sha=00000000:0 malloc=libc bits=64 build=kividb"; exit 0
fi
while [ $# -gt 0 ]; do
  case "$1" in
    --set-proc-title|--save|--daemonize) shift 2 ;;
    --appendonly) [ "$2" = "yes" ] && args+=(--aof); shift 2 ;;
    --pidfile) pidfile="$2"; shift 2 ;;
    *.conf) args+=(--configfile "$1"); shift ;;   # redis-server /path/redis.conf
    *) args+=("$1"); shift ;;
  esac
done
[ -n "$pidfile" ] && echo $$ > "$pidfile"
exec "$BIN" "${args[@]}"
