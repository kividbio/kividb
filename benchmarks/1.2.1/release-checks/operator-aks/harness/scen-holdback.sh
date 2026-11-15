# scen-holdback.sh <scenario> <cluster>: a bad input holds back the writes, not the failover (#36, issue #28).
# spec.configRef is pointed at a KividbConfig that does not exist: Ready must go False (ReconcileError), nothing is
# rendered or rolled out. Then the master is frozen (SIGSTOP): the operator must still fail over. Then the reference
# is removed again and the cluster must go back to Ready without a rollout. Every acknowledged write is read back.
source "$(dirname "$0")/env.sh"
SC=$1; C=${2:-rep}; LDR=loader-$C; E="$D/evidence/$SC"; mkdir -p "$E"
k get pods -l app.kubernetes.io/instance=$C -o custom-columns=N:.metadata.name,UID:.metadata.uid,R:.status.containerStatuses[0].restartCount --no-headers > "$E/pods-before.txt"
h0=$(k get sts $C -o jsonpath='{.spec.template.metadata.annotations}'); echo "$h0" > "$E/sts-annotations-before.txt"
begin $SC; sleep 15
mark "$SC spec.configRef -> missing-cfg (does not exist)"; k patch kividbcluster $C --type merge -p '{"spec":{"configRef":{"name":"missing-cfg"}}}' >/dev/null
sleep 20; echo "Ready after bad ref: $(cond $C Ready)" | tee "$E/conditions.txt"
M=$(mp $C); mark "$SC SIGSTOP master $M"; hsig $M STOP; t0=$(date +%s)
for i in $(seq 1 60); do m=$(mp $C); [ -n "$m" ] && [ "$m" != "$M" ] && break; sleep 2; done
mark "$SC master $M -> $m after $(( $(date +%s)-t0 )) s"
echo "Ready during: $(cond $C Ready)" | tee -a "$E/conditions.txt"
r0=$(grep "^$M " "$E/pods-before.txt" | awk '{print $3}')
for i in $(seq 1 90); do [ "$(restarts $M)" != "$r0" ] && break; sleep 2; done; waitready $M 240; mark "$SC old master $M back (liveness restart)"
mark "$SC spec.configRef removed"; k patch kividbcluster $C --type json -p '[{"op":"remove","path":"/spec/configRef"}]' >/dev/null
for i in $(seq 1 60); do [ "$(k get kividbcluster $C -o jsonpath="{.status.conditions[?(@.type=='Ready')].status}")" = True ] && break; sleep 2; done
mark "$SC Ready again: $(cond $C Ready)"; echo "Ready after fix: $(cond $C Ready)" | tee -a "$E/conditions.txt"
k get sts $C -o jsonpath='{.spec.template.metadata.annotations}' > "$E/sts-annotations-after.txt"
k get pods -l app.kubernetes.io/instance=$C -o custom-columns=N:.metadata.name,UID:.metadata.uid,R:.status.containerStatuses[0].restartCount --no-headers > "$E/pods-after.txt"
k get events --field-selector involvedObject.name=$C -o custom-columns=T:.lastTimestamp,TY:.type,R:.reason,N:.count,M:.message --no-headers | tail -30 > "$E/events.txt"
sleep 10; chk2 $LDR $SC
