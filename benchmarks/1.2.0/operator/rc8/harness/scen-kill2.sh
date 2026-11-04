# scen-kill.sh <scenario> <master pod> <inplace|force>: SIGKILL the engine (host PID; container restarts in place)
# or force-delete the pod (--grace-period=0 --force), wait until Ready, verify
source "$(dirname "$0")/env.sh"
SC=$1; P=$2; HOW=$3; r0=$(restarts $P); uid0=$(k get pod $P -o jsonpath='{.metadata.uid}')
mkdir -p "$D/evidence/$SC"
# what clients are told: CLUSTER SLOTS from another node every 0.5 s for 40 s, from 10 s before the kill
if [ "${LD:-loader-shd}" = loader-shd ]; then OTHER=$(masters | awk -v p=$P '$1!=p {print $1; exit}'); S=${OTHER%-*}
  k exec cli -- sh -c "for i in \$(seq 1 80); do echo \"== \$(date -u +%H:%M:%S.%N | cut -c1-12)\"; redis-cli -h $OTHER.$S-headless.e2e.svc -p 6380 --user default --pass \$(cat /tmp/pw-shd) --no-auth-warning cluster slots | tr '\n' ' '; echo; sleep 0.5; done" > "$D/evidence/$SC/cluster-slots.txt" 2>&1 &
fi
begin $SC; sleep 10
if [ "$HOW" = inplace ]; then
  mark "$SC SIGKILL $P"; hsig $P KILL
  for i in $(seq 1 60); do [ "$(restarts $P)" != "$r0" ] && break; sleep 1; done
else
  mark "$SC force delete $P"; k delete pod $P --grace-period=0 --force 2>&1 | tail -1
  for i in $(seq 1 60); do u=$(k get pod $P -o jsonpath='{.metadata.uid}' 2>/dev/null); [ -n "$u" ] && [ "$u" != "$uid0" ] && break; sleep 1; done
fi
waitready $P 240; mark "$SC $P Ready"
sleep 20; chk2 ${LD:-loader-shd} $SC
masters
