# scen-drain.sh <scenario> <master pod>: drain the node that runs a shard master, wait for every pod Ready, uncordon, verify both loaders
source "$(dirname "$0")/env.sh"
SC=$1; P=$2; N=$(k get pod $P -o jsonpath='{.spec.nodeName}')
echo "node $N: $(k get pods -o wide --no-headers | awk -v n=$N '$7==n {print $1}' | tr '\n' ' ')"
mkdir -p "$D/evidence/$SC"; for x in $(k get pods -o wide --no-headers | awk -v n=$N '$7==n && $1 ~ /^(rep|shd)-/ {print $1}'); do k logs -f $x -c kividb --since=1s > "$D/evidence/$SC/$x-old.log" 2>&1 & done
begin $SC; sleep 15
mark "$SC drain ${N##*-}"; kubectl drain $N --ignore-daemonsets --delete-emptydir-data --timeout=600s 2>&1 | grep -E "evicted|drained|error" 
mark "$SC drained"
for i in $(seq 1 60); do nr=$(k get pods -l app.kubernetes.io/name=kividb --no-headers 2>/dev/null | grep -vc "2/2"); [ "$nr" = 0 ] && break; sleep 5; done
mark "$SC all kividb pods Ready"; kubectl uncordon $N
sleep 20; chk2 loader-shd $SC; cp "$D/t0-$SC" "$D/t0-$SC-rep"; chk2 loader-rep $SC-rep
masters; k get pods -o wide --no-headers | grep -E "^(rep|shd)" | awk '{print $1,$7}'
