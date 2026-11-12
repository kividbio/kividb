#!/usr/bin/env bash
# repro_pause2.sh <kividb-binary>: the remaining steps of "Test RO scripts are not blocked by pause RO"
BIN=$1 IO=${2:-uring} P=6599
D=$(mktemp -d); (cd $D && exec $BIN --port $P --io-model $IO --loglevel warning > $D/log 2>&1) &
for i in $(seq 50); do redis-cli -p $P PING >/dev/null 2>&1 && break; sleep 0.1; done
c() { timeout 3 redis-cli -p $P "$@"; echo "  [exit $?]"; }
echo "--- $BIN io=$IO"
c SET x y
c CLIENT PAUSE 6000000 WRITE
echo "a. EVAL_RO with a key, mixed-case GeT:"; c EVAL_RO "return redis.call('GeT', 'x')..' unique script'" 1 x
echo "b. SCRIPT LOAD (no-writes shebang) while paused:"; c SCRIPT LOAD "#!lua flags=no-writes
return 2"
echo "c. SCRIPT LOAD plain while paused:"; c SCRIPT LOAD "return 3"
echo "d. SCRIPT EXISTS while paused:"; c SCRIPT EXISTS 09d3822de862f46d784e6a36848b4f0736dda47a
echo "e. SCRIPT FLUSH while paused:"; c SCRIPT FLUSH
echo "f. FUNCTION LOAD while paused (Redis holds it):"; c FUNCTION LOAD "#!lua name=f9
redis.register_function('g', function() return 1 end)"
c CLIENT UNPAUSE
kill -9 %1; wait 2>/dev/null; rm -rf $D
