# scen-bootguard.sh <scenario>: bootstrapFromSnapshot only at creation (#37, issue #29). Run after scen-backup.sh.
#  1. Adding spec.bootstrapFromSnapshot to the running cluster `rep` must be refused by the API server (CEL rule),
#     and `rep` must not be touched.
#  2. Delete and re-create: `rep-restored` (restored from the snapshot by scen-backup.sh) gets 1,000 extra keys,
#     is deleted and at once created again with the same bootstrapFromSnapshot. The operator must wait for the old
#     cluster's StatefulSet and pods to be gone (BootstrapWaiting), then restore: the cluster must hold exactly the
#     snapshot's keys, and none of the extra keys (a stale appendonly.aof on a kept volume would bring them back).
source "$(dirname "$0")/env.sh"
SC=$1; E="$D/evidence/$SC"; mkdir -p "$E"; J=$(cat "$D/rep-snap"); PWR=pw-one-1111
r() { k exec cli -- redis-cli -h $1 -p 6380 --user default --pass $PWR --no-auth-warning "${@:2}"; }
mark "$SC 1: add bootstrapFromSnapshot to running rep"
k get pods -l app.kubernetes.io/instance=rep -o custom-columns=N:.metadata.name,UID:.metadata.uid --no-headers > "$E/rep-pods-before.txt"
k patch kividbcluster rep --type merge -p "{\"spec\":{\"bootstrapFromSnapshot\":{\"snapshotRef\":{\"name\":\"$J\"}}}}" > "$E/cel-patch.txt" 2>&1; echo "exit $?" >> "$E/cel-patch.txt"; cat "$E/cel-patch.txt"
sleep 15; k get pods -l app.kubernetes.io/instance=rep -o custom-columns=N:.metadata.name,UID:.metadata.uid --no-headers > "$E/rep-pods-after.txt"
cmp -s "$E/rep-pods-before.txt" "$E/rep-pods-after.txt" && echo "rep pods unchanged" | tee -a "$E/cel-patch.txt"
k get kividbcluster rep -o jsonpath='{.spec.bootstrapFromSnapshot}' | tee -a "$E/cel-patch.txt"; echo
SNAPKEYS=$(r rep-restored-0.rep-restored-headless.e2e.svc dbsize); echo "rep-restored keys (= snapshot) $SNAPKEYS" | tee "$E/recreate.txt"
mark "$SC 2: 1000 extra keys into rep-restored"
k exec cli -- sh -c "for i in \$(seq 1 1000); do echo SET bgextra:\$i x; done | redis-cli -h rep-restored-0.rep-restored-headless.e2e.svc -p 6380 --user default --pass $PWR --no-auth-warning > /dev/null"
sleep 3; echo "with extras $(r rep-restored-0.rep-restored-headless.e2e.svc dbsize)" | tee -a "$E/recreate.txt"
k get pvc --no-headers | grep rep-restored | tee -a "$E/recreate.txt"
t0=$(date +%s); mark "$SC 2: delete rep-restored and create it again at once"
k delete kividbcluster rep-restored --wait=false; k apply -f "$D/rep-restored.yaml"
for i in $(seq 1 120); do echo "$(ts) +$(( $(date +%s)-t0 ))s phase=$(k get kividbcluster rep-restored -o jsonpath='{.status.phase} bootstrap={.status.bootstrap}') pods=$(k get pods --no-headers 2>/dev/null | grep -E '^rep-restored' | awk '{print $1":"$3}' | tr '\n' ' ')" >> "$E/watch.txt"
  [ "$(k get kividbcluster rep-restored -o jsonpath='{.status.phase}')" = Running ] && break; sleep 5; done
mark "$SC 2: rep-restored Running again after $(( $(date +%s)-t0 )) s"
sleep 30
for p in rep-restored-0 rep-restored-1; do echo "$p keys $(r $p.rep-restored-headless.e2e.svc dbsize) extras $(k exec cli -- sh -c "redis-cli -h $p.rep-restored-headless.e2e.svc -p 6380 --user default --pass $PWR --no-auth-warning --scan --pattern 'bgextra:*' --count 1000 | wc -l")"; done | tee -a "$E/recreate.txt"
k get events --field-selector involvedObject.name=rep-restored -o custom-columns=T:.lastTimestamp,TY:.type,R:.reason,M:.message --no-headers > "$E/events.txt"
k logs -l job-name --tail=5 --prefix 2>/dev/null | grep -i restore | tail -5 >> "$E/recreate.txt"
echo BOOTGUARD_DONE
