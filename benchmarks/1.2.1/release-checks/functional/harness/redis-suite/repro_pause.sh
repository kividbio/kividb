#!/usr/bin/env bash
# repro_pause.sh <kividb-binary> [io-model]: the step of Redis's "Test RO scripts are not blocked by pause RO"
# (tests/unit/pause.tcl) that hangs, by hand with redis-cli.
BIN=$1 IO=${2:-uring} P=6599
D=$(mktemp -d); (cd $D && exec $BIN --port $P --io-model $IO --loglevel warning > $D/log 2>&1) &
for i in $(seq 50); do redis-cli -p $P PING >/dev/null 2>&1 && break; sleep 0.1; done
c() { timeout 3 redis-cli -p $P "$@"; echo "  [exit $?]"; }
echo "--- $($BIN --version) io=$IO"
c SET x y
c FUNCTION LOAD REPLACE "#!lua name=f1
redis.register_function{function_name='f1', callback=function() return 'hello' end, flags={'no-writes'}}"
c CLIENT PAUSE 6000000 WRITE
echo "1. EVAL with #!lua flags=no-writes, not in the script cache:"
c EVAL "#!lua flags=no-writes
return 'unique script'" 0
echo "2. the same again (cached):"
c EVAL "#!lua flags=no-writes
return 'unique script'" 0
echo "3. EVAL_RO plain:"
c EVAL_RO "return redis.call('get','x')" 0
echo "4. FCALL f1 (no-writes):"
c FCALL f1 0
echo "5. FCALL_RO f1:"
c FCALL_RO f1 0
echo "6. EVAL without a shebang (a write script) must block (timeout expected):"
c EVAL "return 1" 0
c CLIENT UNPAUSE
kill -9 %1; wait 2>/dev/null; rm -rf $D
