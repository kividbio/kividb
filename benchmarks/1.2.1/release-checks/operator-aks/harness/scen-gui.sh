# scen-gui.sh: B8, the console. Read-only API first, then sign-in (gui.auth.existingSecret) and a promote of a shard
# replica from the GUI under load, verified like any handover. (c3 run)
source "$(dirname "$0")/env.sh"
SC=B8-gui; E="$D/evidence/$SC"; mkdir -p "$E"
G=http://kividb-operator-gui.kividb-system.svc:8090
g() { k exec curl -- curl -s -m 30 -o /dev/null -w "%{http_code}" "$G$1"; }
for u in /healthz /api/session /api/clusters /api/clusters/e2e/shd /api/clusters/e2e/shd/live /api/clusters/e2e/shd/dbops /api/create-options; do echo "GET $u $(g $u)"; done | tee "$E/readonly.txt"
k exec curl -- curl -s -m 30 $G/api/session | tee "$E/session-ro.json"; echo
k exec curl -- curl -s -m 30 $G/api/clusters/e2e/shd > "$E/shd.json"; head -c 1500 "$E/shd.json"; echo
k exec curl -- curl -s -m 30 -o /dev/null -w "promote without sign-in: %{http_code}\n" -X POST -H 'Content-Type: application/json' -d '{"pod":"shd-0-0"}' $G/api/clusters/e2e/shd/promote | tee -a "$E/readonly.txt"
kubectl -n kividb-system create secret generic gui-auth --from-literal=username=admin --from-literal=password=gui-pw-c3-1111 --dry-run=client -o yaml | kubectl apply -f -
helm upgrade kividb-operator /h/chart -n kividb-system --reuse-values --set gui.auth.existingSecret=gui-auth --wait --timeout 5m | tail -3
kubectl -n kividb-system rollout status deploy -l app.kubernetes.io/component=gui --timeout=180s 2>/dev/null || sleep 30
for i in $(seq 1 30); do [ "$(g /healthz)" = 200 ] && break; sleep 3; done
k exec curl -- sh -c "curl -s -m 30 -c /tmp/ck -o /dev/null -w 'login %{http_code}\n' -d username=admin -d password=gui-pw-c3-1111 $G/login; curl -s -m 30 -b /tmp/ck $G/api/session" | tee "$E/session-rw.txt"; echo
# promote the replica of shard 1 (a planned handover, CLUSTER FAILOVER) under load
M=$(smaster 1); R=$(k get pods -l app.kubernetes.io/instance=shd -o name | sed 's|pod/||' | grep '^shd-1-' | grep -v "^$M$" | head -1)
begin $SC; sleep 15
mark "$SC promote $R (master $M) from the GUI"
k exec curl -- curl -s -m 30 -b /tmp/ck -X POST -H 'Content-Type: application/json' -d "{\"pod\":\"$R\"}" -w " %{http_code}\n" $G/api/clusters/e2e/shd/promote | tee "$E/promote.txt"
for i in $(seq 1 30); do [ "$(smaster 1)" = "$R" ] && break; sleep 2; done
mark "$SC shard 1 master now $(smaster 1)"
sleep 20; chk2 loader-shd $SC
masters
