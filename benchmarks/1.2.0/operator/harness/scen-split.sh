# scen-split.sh <scenario>: delete one REPLICA pod of shard 0 and one of shard 1 at the same moment (both come back
# with new pod IPs), then check every node's view of every peer's address. Repeats up to 3 times.
source "$(dirname "$0")/env.sh"
SC=$1; mkdir -p "$D/evidence/$SC"
for n in 1 2 3; do
  r0=$(k get pods -l app.kubernetes.io/instance=shd,kividb.io/shard=0,kividb.io/role=replica -o jsonpath='{.items[0].metadata.name}')
  r1=$(k get pods -l app.kubernetes.io/instance=shd,kividb.io/shard=1,kividb.io/role=replica -o jsonpath='{.items[0].metadata.name}')
  mark "$SC try $n: delete $r0 and $r1 together"; k delete pod $r0 $r1 --wait=false
  sleep 8; waitready $r0 240; waitready $r1 240; sleep 30
  echo "--- try $n $(ts): $r0 $r1"; views | tee "$D/evidence/$SC/views-try$n.txt"
  k exec cli -- sh -c "P=\$(cat /tmp/pw-shd); for h in $r0.shd-0 $r1.shd-1; do echo \"== \$h\"; redis-cli -h \$h-headless.e2e.svc -p 6380 --user default --pass \$P --no-auth-warning cluster nodes; done" > "$D/evidence/$SC/nodes-try$n.txt" 2>&1
  grep -q "stale-ip" "$D/evidence/$SC/views-try$n.txt" && { mark "$SC try $n: SPLIT reproduced"; break; }
done
