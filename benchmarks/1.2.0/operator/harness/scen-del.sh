# scen-del.sh <scenario> <master pod>: graceful kubectl delete of a shard master (preStop handover), following its log to the end
source "$(dirname "$0")/env.sh"
SC=$1; P=$2; uid0=$(k get pod $P -o jsonpath='{.metadata.uid}')
mkdir -p "$D/evidence/$SC"; k logs -f $P -c kividb --since=1s > "$D/evidence/$SC/$P-old.log" 2>&1 &
k logs -f $P -c agent --since=1s > "$D/evidence/$SC/$P-old-agent.log" 2>&1 &
begin $SC; sleep 15
mark "$SC delete $P"; k delete pod $P --wait=false
for i in $(seq 1 90); do u=$(k get pod $P -o jsonpath='{.metadata.uid}' 2>/dev/null); [ -n "$u" ] && [ "$u" != "$uid0" ] && break; sleep 1; done
mark "$SC old $P gone"
waitready $P 240; mark "$SC $P Ready"
sleep 20; chk2 ${LD:-loader-shd} $SC
masters
