# scen-shrink.sh <scenario> <a|b>: a shard shrink whose finishing status write is lost (#38, issue #30).
# 3 -> 4 shards, then 4 -> 3 while the status is read every second; the last status seen in phase Rebalancing is kept.
# Once the shrink has finished (removed shard's StatefulSet, PDB and PVCs gone, phase Running), the operator is
# stopped and that kept status is written back through the status subresource: the state a lost finish leaves.
#   a: the operator is started again; it must finish the shrink again, without waiting for the removed shard.
#   b: spec.shards is raised back to 4 first, then the operator is started; the re-created shard must get its
#      -0 as master and its share of slots (before the fix it stayed in AddingShards for good).
# Every acknowledged write is read back.
source "$(dirname "$0")/env.sh"
SC=$1; MODE=$2; E="$D/evidence/$SC"; mkdir -p "$E"; OPNS=kividb-system; OPD=kividb-operator-manager
ph() { k get kividbcluster shd -o jsonpath='{.status.phase}'; }
waitrun() { local n=$1 i; for i in $(seq 1 ${2:-120}); do np=$(k get pods -l app.kubernetes.io/instance=shd --no-headers | wc -l); [ "$np" = $((n*2)) ] && [ "$(ph)" = Running ] && return 0; sleep 5; done; return 1; }
begin $SC; sleep 15
mark "$SC spec.shards=4"; k patch kividbcluster shd --type merge -p '{"spec":{"shards":4}}' >/dev/null; sleep 10; waitrun 4; mark "$SC Running with 4 shards"
( while true; do k get kividbcluster shd -o json > "$E/.cur.json" 2>/dev/null && python -c "import json,sys; o=json.load(open(sys.argv[1])); sys.exit(0 if o.get('status',{}).get('phase')=='Rebalancing' else 1)" "$E/.cur.json" && cp "$E/.cur.json" "$E/last-rebalancing.json"; sleep 1; done ) & CAP=$!
mark "$SC spec.shards=3"; k patch kividbcluster shd --type merge -p '{"spec":{"shards":3}}' >/dev/null; sleep 10; waitrun 3; mark "$SC shrink finished (Running, 6 pods)"
kill $CAP 2>/dev/null; sleep 2
python -c "import json,sys; s=json.load(open(sys.argv[1]))['status']; print('kept status: phase', s.get('phase'), 'shardCount', s.get('shardCount'), 'rebalance', json.dumps(s.get('rebalance'))[:400])" "$E/last-rebalancing.json" | tee "$E/kept-status.txt"
k get sts,pdb,pvc --no-headers 2>/dev/null | grep -E "shd-3" > "$E/shard3-left.txt"; echo "shard 3 objects left: $(wc -l < "$E/shard3-left.txt")"
mark "$SC operator stopped"; kubectl -n $OPNS scale deploy/$OPD --replicas=0 >/dev/null; kubectl -n $OPNS wait --for=delete pod -l app.kubernetes.io/component=manager --timeout=120s >/dev/null 2>&1
k get kividbcluster shd -o json > "$E/.now.json"
python -c "import json,sys; o=json.load(open(sys.argv[1])); o['status']=json.load(open(sys.argv[2]))['status']; json.dump(o,open(sys.argv[3],'w'))" "$E/.now.json" "$E/last-rebalancing.json" "$E/stale.json"
k replace --subresource=status -f "$E/stale.json" >/dev/null && mark "$SC stale status written back (phase $(ph))"
if [ "$MODE" = b ]; then mark "$SC spec.shards=4 while the finish is lost"; k patch kividbcluster shd --type merge -p '{"spec":{"shards":4}}' >/dev/null; N=4; else N=3; fi
t0=$(date +%s); mark "$SC operator started"; kubectl -n $OPNS scale deploy/$OPD --replicas=1 >/dev/null
for i in $(seq 1 120); do echo "$(ts) +$(( $(date +%s)-t0 ))s phase=$(ph) pods=$(k get pods -l app.kubernetes.io/instance=shd --no-headers | wc -l) Ready=$(cond shd Ready)" >> "$E/watch.txt"; sleep 5; waitrun $N 1 && break; done
if waitrun $N 1; then mark "$SC recovered: Running with $N shards after $(( $(date +%s)-t0 )) s"; else mark "$SC NOT recovered after $(( $(date +%s)-t0 )) s: phase $(ph)"; fi
masters | tee "$E/masters.txt"; k exec cli -- redis-cli -h shd -p 6380 --user default --pass shd-pw-1111 --no-auth-warning cluster info | grep -E "cluster_state|cluster_size|slots_assigned|known_nodes" | tee "$E/cluster-info.txt"
k get kividbcluster shd -o jsonpath='{.status}' > "$E/status-after.json"
sleep 20; chk2 loader-shd $SC
