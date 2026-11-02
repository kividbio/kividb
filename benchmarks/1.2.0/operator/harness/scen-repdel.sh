# scen-repdel.sh <scenario> <shard>: graceful delete of a shard's REPLICA pod; follow the master's log and its INFO replication
source "$(dirname "$0")/env.sh"
SC=$1; S=$2; M=$(smaster $S); R=$(k get pods -l app.kubernetes.io/instance=shd --no-headers -o name | sed 's|pod/||' | grep "^shd-$S-" | grep -v "^$M$" | head -1)
uid0=$(k get pod $R -o jsonpath='{.metadata.uid}'); mkdir -p "$D/evidence/$SC"; echo "master $M replica $R"
k logs -f $M -c kividb --since=1s > "$D/evidence/$SC/$M-master.log" 2>&1 &
k exec cli -- sh -c "for i in \$(seq 1 70); do echo \"== \$(date -u +%H:%M:%S.%N | cut -c1-12)\"; timeout 2 redis-cli -h $M.shd-$S-headless.e2e.svc -p 6380 --user default --pass \$(cat /tmp/pw-shd) --no-auth-warning info replication | tr -d '\r' | grep -E 'connected_slaves|slave0|master_repl_offset' | tr '\n' ' '; echo; sleep 0.5; done" > "$D/evidence/$SC/master-info-replication.txt" 2>&1 &
begin $SC; sleep 15
mark "$SC delete replica $R"; k delete pod $R --wait=false
for i in $(seq 1 90); do u=$(k get pod $R -o jsonpath='{.metadata.uid}' 2>/dev/null); [ -n "$u" ] && [ "$u" != "$uid0" ] && break; sleep 1; done
waitready $R 240; mark "$SC $R Ready"
sleep 20; chk2 loader-shd $SC
