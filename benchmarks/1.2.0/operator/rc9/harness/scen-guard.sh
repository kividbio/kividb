# scen-guard.sh <scenario> <cluster> <loader> [opdown|opup]: the #42 AOF-master guard on a replicated cluster.
# The master's engine is frozen (SIGSTOP), its pod force-deleted and the engine SIGKILLed, so it never shuts down
# cleanly (a clean stop would carry on its replication stream) and comes back in a NEW pod (new IP) from its own
# volume and AOF, in a new stream. Its replica still follows the old address.
#   opdown (default): the operator is scaled to 0 first and back to 1 once the new pod is Ready, so the operator's
#     first look is at a restarted master that is Ready with a new stream: the case regressedMaster decides
#     (keep the master, or MasterRestartedBehind + failover to the replica).
#   opup: the operator keeps running (it sees the master pod gone and fails over at once, before any guard).
# Records: who is master before/after, stream ids, key counts, versions and aof-ack-after-write of both pods,
# the operator's Events and log lines, and every acknowledged write read back.
source "$(dirname "$0")/env.sh"
SC=$1; C=$2; LDR=$3; HOW=${4:-opdown}; E="$D/evidence/$SC"; mkdir -p "$E"
OPNS=kividb-system; OPD=kividb-operator-manager
M=$(mp $C); uid0=$(k get pod $M -o jsonpath='{.metadata.uid}'); ip0=$(k get pod $M -o jsonpath='{.status.podIP}')
echo "master $M ip $ip0" | tee "$E/before.txt"; rinfo $C | tee -a "$E/before.txt"
k get kividbcluster $C -o jsonpath='{.status.pods}' > "$E/status-pods-before.json"
begin $SC; sleep 15
if [ "$HOW" = opdown ]; then
  mark "$SC operator scaled to 0"; kubectl -n $OPNS scale deploy/$OPD --replicas=0 >/dev/null
  kubectl -n $OPNS wait --for=delete pod -l app.kubernetes.io/component=manager --timeout=120s >/dev/null 2>&1 || sleep 20
fi
set -- $(hpid $M); ND=$1; PID=$2
mark "$SC SIGSTOP + force delete + SIGKILL $M (pid $PID)"
k exec $ND -- kill -STOP $PID; k delete pod $M --grace-period=0 --force >/dev/null 2>&1; k exec $ND -- kill -KILL $PID 2>/dev/null
for i in $(seq 1 90); do u=$(k get pod $M -o jsonpath='{.metadata.uid}' 2>/dev/null); [ -n "$u" ] && [ "$u" != "$uid0" ] && break; sleep 1; done
waitready $M 300; mark "$SC new $M Ready ($(k get pod $M -o jsonpath='{.status.podIP}'))"
echo "--- new pod Ready, operator $HOW $(ts)" >> "$E/before.txt"; rinfo $C | tee "$E/at-ready.txt"
if [ "$HOW" = opdown ]; then
  mark "$SC operator scaled to 1"; kubectl -n $OPNS scale deploy/$OPD --replicas=1 >/dev/null
fi
for i in $(seq 1 24); do echo "$(ts) masterPod=$(mp $C) phase=$(k get kividbcluster $C -o jsonpath='{.status.phase}')"; sleep 5; done | tee "$E/master-watch.txt"
M2=$(mp $C); mark "$SC master after: $M2"
rinfo $C | tee "$E/after.txt"
k logs $M -c kividb > "$E/$M-new.log" 2>&1
grep -E "AOF|Replication|stream|Loaded|Dropped|resync" "$E/$M-new.log" | head -40 > "$E/$M-new-key-lines.txt"
evsince $(cat "$D/t0-$SC") | grep -E " $C( |-)" > "$E/events.txt"
kubectl -n $OPNS logs deploy/$OPD --since=5m 2>/dev/null | grep -E "\"$C\"|$C-" | grep -iE "restarted behind|failover|promot|elect|switchover|replicaof" > "$E/operator-lines.txt"
if [ "$M2" = "$M" ] && ! grep -q MasterRestartedBehind "$E/events.txt"; then V=KEPT; else V=FAILED_OVER; fi
echo "$SC verdict $V (master $M -> $M2)" | tee "$E/verdict.txt"; mark "$SC verdict $V"
sleep 10; chk2 $LDR $SC
