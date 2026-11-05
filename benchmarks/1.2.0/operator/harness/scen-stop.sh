# scen-stop.sh <scenario> <master pod>: SIGSTOP a shard master's engine (host PID), wait for the restart, verify
source "$(dirname "$0")/env.sh"
SC=$1; P=$2; r0=$(restarts $P)
begin $SC; sleep 15
mark "$SC SIGSTOP $P"; hsig $P STOP
for i in $(seq 1 120); do [ "$(restarts $P)" != "$r0" ] && break; sleep 2; done
mark "$SC $P container restarted"
waitready $P 240; mark "$SC $P Ready"
sleep 20; chk2 ${LD:-loader-shd} $SC
masters
