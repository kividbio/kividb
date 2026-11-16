# scen-c1.sh: C1, a KividbConfig with an unknown directive on the replicated cluster under load. Only the first pod
# rolled may fail (CrashLoopBackOff), the master and the other replica must stay; then the directive is removed and
# the roll finishes. (c3 run)
source "$(dirname "$0")/env.sh"
SC=C1-bad-directive; E="$D/evidence/$SC"; mkdir -p "$E"
begin $SC; sleep 15
printf 'apiVersion: kividb.io/v1alpha1\nkind: KividbConfig\nmetadata: {name: rep-cfg, namespace: e2e}\nspec:\n  directives:\n    maxmemory-policy: allkeys-lru\n    no-such-directive-xyz: "yes"\n' | k apply -f -
mark "$SC configRef -> rep-cfg (bad directive)"; k patch kividbcluster rep --type merge -p '{"spec":{"configRef":{"name":"rep-cfg"}}}'
sleep 100
k get pods -l app.kubernetes.io/instance=rep -o wide > "$E/pods-bad.txt"; cat "$E/pods-bad.txt"
k get kividbcluster rep -o yaml | sed -n '/^status:/,$p' > "$E/status-bad.yaml"
for p in rep-0 rep-1 rep-2; do echo "$p: $(k logs $p -c kividb --tail=3 2>&1 | grep -i -E 'fatal|directive' | tail -1)"; done | tee "$E/logs-bad.txt"
rinfo rep | tee "$E/rinfo-bad.txt"
mark "$SC directive removed"
printf 'apiVersion: kividb.io/v1alpha1\nkind: KividbConfig\nmetadata: {name: rep-cfg, namespace: e2e}\nspec:\n  directives:\n    maxmemory-policy: allkeys-lru\n' | k apply -f -
uids0=$(k get pods -l app.kubernetes.io/instance=rep -o jsonpath='{range .items[*]}{.metadata.uid}{"\n"}{end}' | sort)
for i in $(seq 1 180); do
  rdy=$(k get pods -l app.kubernetes.io/instance=rep -o jsonpath='{range .items[*]}{.status.containerStatuses[?(@.name=="kividb")].ready}{"\n"}{end}' | grep -c true)
  ph=$(k get kividbcluster rep -o jsonpath='{.status.phase}'); pol=$(rc $(mp rep).rep-headless.e2e.svc config get maxmemory-policy 2>/dev/null | tail -1)
  [ "$rdy" = 3 ] && [ "$ph" = Running ] && [ "$pol" = allkeys-lru ] && { all=1; for p in rep-0 rep-1 rep-2; do [ "$(rc $p.rep-headless.e2e.svc config get maxmemory-policy 2>/dev/null | tail -1)" = allkeys-lru ] || all=0; done; [ $all = 1 ] && break; }
  sleep 5; done
mark "$SC rolled: ready=$rdy phase=$ph policy=$pol"
sleep 20; chk2 loader-rep $SC
