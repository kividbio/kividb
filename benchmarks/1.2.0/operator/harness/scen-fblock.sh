# scen-fblock.sh <scenario> <cluster>: FailoverBlocked must hold for as long as the only Ready replica is empty (#35, issue #27).
# The master holding keys is frozen (SIGSTOP). Its replica's data directory is wiped and its engine killed, so it
# restarts in place, empty, and cannot sync from the frozen master: a Ready replica with 0 keys. The operator must
# block the failover on every pass (FailoverBlocked), not promote the empty replica on the second pass, until liveness
# restarts the master (~90 s); then the replica syncs from it. Every 3 s: master, Ready condition, both pods' keys, the
# operator's status.pods and the Events. Then every acknowledged write is read back.
source "$(dirname "$0")/env.sh"
SC=$1; C=${2:-fb}; LDR=loader-$C; E="$D/evidence/$SC"; mkdir -p "$E"
M=$(mp $C); R=$(k get pods -l app.kubernetes.io/instance=$C -o name | sed 's|pod/||' | grep -v "^$M$" | head -1); r0=$(restarts $M)
echo "master $M replica $R" | tee "$E/before.txt"; rinfo $C | tee -a "$E/before.txt"
begin $SC; sleep 15
mark "$SC SIGSTOP master $M"; hsig $M STOP
set -- $(hpid $R); ND=$1; PID=$2
mark "$SC wipe replica $R data dir + SIGKILL (restarts empty)"
k exec $ND -- sh -c "ls -la /proc/$PID/root/data/; rm -rf /proc/$PID/root/data/* /proc/$PID/root/data/.[!.]* ; kill -9 $PID; echo wiped" > "$E/wipe.txt" 2>&1
t0=$(date +%s); back=""
for i in $(seq 1 80); do
  kr=$(k exec cli -- sh -c "timeout 2 redis-cli -h $R.$C-headless.e2e.svc -p 6380 --user default --pass \$(cat /tmp/pw) --no-auth-warning dbsize" 2>&1 | tr -d '\r')
  st=$(k get kividbcluster $C -o jsonpath='{.status.masterPod} {.status.phase} {range .status.pods[*]}{.name}:{.role}:keys={.keys}:ready={.ready}:synced={.synced} {end}')
  nb=$(grep -c "FailoverBlocked.* $C " "$D/events.log" 2>/dev/null); nb2=$(k get events --field-selector involvedObject.name=$C,reason=FailoverBlocked -o jsonpath='{.items[0].count}' 2>/dev/null)
  rd=$(k get pod $R -o jsonpath='{.status.containerStatuses[?(@.name=="kividb")].ready}')
  echo "$(ts) +$(( $(date +%s)-t0 ))s replicaReady=$rd replicaKeys=$kr | $st | Ready=$(cond $C Ready) | FailoverBlocked events=$nb2" >> "$E/watch.txt"
  if [ -z "$back" ] && [ "$(restarts $M)" != "$r0" ]; then back=1; mark "$SC master $M restarted by liveness"; fi
  [ -n "$back" ] && [ "$(k get pod $M -o jsonpath='{.status.containerStatuses[?(@.name=="kividb")].ready}')" = true ] && [ "$kr" != 0 ] && [ -n "$kr" ] && break
  sleep 3
done
mark "$SC master $(mp $C), replica keys $kr"
sleep 15; rinfo $C | tee "$E/after.txt"
k get events --field-selector involvedObject.name=$C -o custom-columns=T:.lastTimestamp,TY:.type,R:.reason,N:.count,M:.message --no-headers > "$E/events.txt"
kubectl -n kividb-system logs deploy/kividb-operator-manager --since=5m 2>/dev/null | grep -E "\"$C\"" | grep -iE "block|failover|promot|elect" | tail -40 > "$E/operator-lines.txt"
chk2 $LDR $SC
