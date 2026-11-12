# scen-dbq.sh <scenario> <cluster>: the DbOps restart queue (#40). Two KividbDbOps restarts of the same cluster created
# in one command; the second must wait (Pending, condition Queued naming the first) and start only when the first is
# done. Every second: both ops' phases and conditions, and how many of the cluster's pods are not Ready or terminating
# (never more than one may be). Then every acknowledged write is read back.
source "$(dirname "$0")/env.sh"
SC=$1; C=$2; LDR=loader-shd; [ "$C" = rep ] && LDR=loader-rep; E="$D/evidence/$SC"; mkdir -p "$E"
a=$(echo "$C-q1-$(date +%H%M%S)" | tr 'A-Z' 'a-z'); b=${a/q1/q2}
begin $SC; sleep 15
mark "$SC two DbOps restarts at once: $a $b"
printf 'apiVersion: kividb.io/v1alpha1\nkind: KividbDbOps\nmetadata: {name: %s, namespace: e2e}\nspec:\n  clusterRef: {name: %s}\n  op: restart\n---\napiVersion: kividb.io/v1alpha1\nkind: KividbDbOps\nmetadata: {name: %s, namespace: e2e}\nspec:\n  clusterRef: {name: %s}\n  op: restart\n' $a $C $b $C | k apply -f - >/dev/null
N=$(( $(k get kividbcluster $C -o jsonpath='{.spec.replicas}') + 1 )); maxdown=0; sa=""; sb=""
for i in $(seq 1 900); do
  pa=$(k get kividbdbops $a -o jsonpath='{.status.phase}'); pb=$(k get kividbdbops $b -o jsonpath='{.status.phase}')
  qb=$(k get kividbdbops $b -o jsonpath='{range .status.conditions[*]}{.type}={.status}:{.message}; {end}')
  up=$(k get pods -l app.kubernetes.io/instance=$C -o jsonpath='{range .items[*]}{.metadata.name}|{.metadata.deletionTimestamp}|{.status.containerStatuses[?(@.name=="kividb")].ready}{"\n"}{end}' | awk -F'|' '$2=="" && $3=="true"' | grep -c .)
  down=$((N - up))
  [ "$down" -gt "$maxdown" ] && maxdown=$down
  echo "$(ts) $a=$pa $b=$pb down=$down | $b: $qb" >> "$E/watch.txt"
  [ "$pa" = Running ] && [ -z "$sa" ] && { sa=$(date +%s); mark "$SC $a Running"; }
  [ "$pb" = Running ] && [ -z "$sb" ] && { sb=$(date +%s); mark "$SC $b Running"; }
  case "$pa$pb" in *Failed*) break;; CompletedCompleted) break;; esac
  sleep 1
done
mark "$SC $a $pa, $b $pb, at most $maxdown pod(s) down at once"
k get kividbdbops $a $b -o yaml > "$E/dbops.yaml"
echo "max pods down at once: $maxdown" | tee "$E/summary.txt"
grep -m1 "$b=Pending" "$E/watch.txt" | tee -a "$E/summary.txt"
sleep 20; chk2 $LDR $SC
