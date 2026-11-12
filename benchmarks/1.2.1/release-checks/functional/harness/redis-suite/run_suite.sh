#!/usr/bin/env bash
# run_suite.sh <binary> <io-model uring|tokio> <outdir> [port]
# Runs scripts/redis_compat.sh the way CI does, with the I/O model forced and checked.
set -u
BIN=$1 IO=$2 OUT=$3 PORT=${4:-6399}
ulimit -n 65535
pkill -9 -x kividb-lua[+]tls 2>/dev/null; pkill -9 -x kividb 2>/dev/null; sleep 1
# Which I/O model does the server really run? (redis_compat.sh starts it at --loglevel error.)
d=$(mktemp -d); (cd $d && timeout 5 "$BIN" --port $((PORT+1000)) --io-model "$IO" --loglevel notice > "$OUT.iomodel.log" 2>&1); rm -rf $d
grep -a -i "I/O model" "$OUT.iomodel.log" | head -2
mkdir -p "$OUT"
cd ~/redis-tests
PORT=$PORT SERVER_ARGS="--io-model $IO" bash ~/src/scripts/redis_compat.sh "$BIN" ~/redis-tests "$OUT" > "$OUT.console.log" 2>&1
tail -3 "$OUT.console.log"
echo SUITE-DONE
