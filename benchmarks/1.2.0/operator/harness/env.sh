export D="${D:-$(pwd)/work}"   # run directory: kubeconfig, logs, res/, evidence/
export KUBECONFIG="$D/kubeconfig" MSYS_NO_PATHCONV=1 A=${A:?set A to your ACR login server} RG=kividb-rc4-aks OPC=${OPC:-eefe65d}
export ENG=${ENG:-ghcr.io/kividbio/kividb:v1.2.0-rc4}
k() { kubectl -n e2e "$@"; }
q() { kubectl -n e2e exec curl -- curl -s -m 900 "http://$1.e2e.svc:8080$2"; }
ts() { date -u +%H:%M:%S; }
mark() { echo "$* $(ts) $(date +%s)" >> "$D/marks.log"; echo "$* $(ts)"; }
begin() { date +%s > "$D/t0-$1"; mark "BEGIN $1"; }
# chk2 <loader> <scenario>: stats, stop, verify, timeline since begin, restart loader
chk2() { local t0=$(cat "$D/t0-$2" 2>/dev/null || echo 0); echo "STOP $2 $(ts) $(date +%s)" >> "$D/marks.log"; q $1 /stats > "$D/res/$2-stats.json"; q $1 /stop >/dev/null; q $1 /verify > "$D/res/$2-verify.json"; q $1 "/timeline?since=$((t0-60))" > "$D/res/$2-timeline.json"; q $1 /reset-errors >/dev/null; q $1 /start >/dev/null; mark "END $2"; python "$D/show.py" "$D/res/$2-stats.json" "$D/res/$2-verify.json"; }
ev() { k get events --sort-by=.lastTimestamp -o custom-columns=T:.lastTimestamp,TY:.type,R:.reason,O:.involvedObject.name,M:.message --no-headers | tail -${1:-25}; }
hpid() { local node=$(k get pod $1 -o jsonpath='{.spec.nodeName}'); local nd=$(k get pod -l app=nd -o jsonpath="{.items[?(@.spec.nodeName=='$node')].metadata.name}"); echo $nd $(k exec $nd -- sh -c "for p in \$(pidof kividb); do tr '\0' '\n' < /proc/\$p/environ | grep -qx HOSTNAME=$1 && echo \$p; done"); }
hsig() { set -- "$1" "$2" $(hpid $1); k exec $3 -- sh -c "kill -$2 $4 && echo $1 pid $4 SIG$2 \$(grep State /proc/$4/status 2>/dev/null)"; }
mp() { k get kividbcluster ${1:-rep} -o jsonpath="{.status.masterPod}"; }
rc() { local h=$1; shift; k exec cli -- redis-cli -h $h -p 6380 --user default --pass "$PW" --no-auth-warning "$@"; }
masters() { k exec cli -- sh -c "redis-cli -h shd -p 6380 --user default --pass \$(cat /tmp/pw-shd) --no-auth-warning cluster nodes" | awk '$3 ~ /master/ {split($2,a,","); h=a[2]; sub(/\..*/,"",h); print h, $9, $10, $11}'; }
# rollfo <scenario>: CLUSTER FAILOVER on the replica of every shard in turn (engine-level rolling failover)
rollfo() { local sc=$1; for s in 0 1 2 3; do
  local pods=$(k get pods -l app.kubernetes.io/instance=shd -o name | sed 's|pod/||' | grep "^shd-$s-"); [ -z "$pods" ] && continue
  for p in $pods; do local role=$(k exec cli -- redis-cli -h $p.shd-$s-headless.e2e.svc -p 6380 --user default --pass $(k exec cli -- cat /tmp/pw-shd) --no-auth-warning role | head -1 | tr -d '\r');
    if [ "$role" = slave ]; then mark "$sc FAILOVER $p"; k exec cli -- redis-cli -h $p.shd-$s-headless.e2e.svc -p 6380 --user default --pass $(k exec cli -- cat /tmp/pw-shd) --no-auth-warning cluster failover; sleep 20; break; fi; done; done; }
# waitready <pod> <max s>: until the pod is Ready again after a restart
waitready() { local i; for i in $(seq 1 $(( ${2:-300} / 5 ))); do [ "$(k get pod $1 -o jsonpath='{.status.containerStatuses[?(@.name=="kividb")].ready}' 2>/dev/null)" = true ] && return 0; sleep 5; done; return 1; }
restarts() { k get pod $1 -o jsonpath='{.status.containerStatuses[?(@.name=="kividb")].restartCount}'; }
# smaster <shard>: pod name of the shard's master
smaster() { masters | awk -v s="shd-$1-" 'index($1,s)==1 {print $1}'; }
# a master of the sharded cluster whose node is not the infra node (so a drain never evicts the loaders)
drainable_master() { local m; for m in $(masters | awk '{print $1}'); do n=$(k get pod $m -o jsonpath='{.spec.nodeName}'); [ "$(kubectl get node $n -o jsonpath='{.metadata.labels.e2e-infra}')" = true ] || { echo $m; return; }; done; }
smaster() { local i m; for i in 1 2 3 4 5 6; do m=$(masters | awk -v s="shd-$1-" 'index($1,s)==1 {print $1}'); [ -n "$m" ] && { echo $m; return; }; sleep 3; done; }
# views: every sharded node's cluster_state and the peers it holds as failed or at an address that is not the pod's
views() { k exec cli -- sh -c 'P=$(cat /tmp/pw-shd); for h in $(getent hosts $(for s in 0 1 2 3; do for r in 0 1; do echo shd-$s-$r.shd-$s-headless.e2e.svc; done; done) 2>/dev/null | awk "{print \$2}"); do st=$(timeout 2 redis-cli -h $h -p 6380 --user default --pass $P --no-auth-warning cluster info | tr -d "\r" | grep cluster_state); bad=$(timeout 2 redis-cli -h $h -p 6380 --user default --pass $P --no-auth-warning cluster nodes | while read id addr fl rest; do ip=${addr%%:*}; hn=${addr##*,}; real=$(getent hosts $hn | cut -d" " -f1); [ -n "$real" ] && [ "$real" != "$ip" ] && echo -n "${hn%%.*}@stale-ip "; case $fl in *fail*) echo -n "${hn%%.*}:$fl ";; esac; done); echo "${h%%.*} $st $bad"; done'; }
