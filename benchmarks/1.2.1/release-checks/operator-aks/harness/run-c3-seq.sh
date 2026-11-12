# run-c3-seq.sh: after both tracks: drain (both clusters), bulk load + reshard 3->4->3, memory, replicaValidityFactor,
# GUI, backups/restores (A5 + B6), memory again, a second reshard cycle.
cd "$(dirname "$0")"; source ./env.sh
export PW=$(k get secret rep-pw -o jsonpath='{.data.password}' | base64 -d)
v() { echo "--- views after $1 $(ts)"; views; }
# A2 drain of the replicated master's node; never the infra node (loaders, cli): hand the master over first if needed
for i in 1 2; do M=$(mp rep); N=$(k get pod $M -o jsonpath='{.spec.nodeName}'); [ "$(kubectl get node $N -o jsonpath='{.metadata.labels.e2e-infra}')" = true ] || break
  echo "rep master $M on the infra node: DbOps restart to move it"; bash scen-dbops.sh A2-premove-$i rep; done
bash scen-drain.sh A2-drain $(mp rep); v A2
bash scen-mem.sh before-bulk
bash bulk.sh 2500000
bash scen-mem.sh after-bulk
bash scen-reshard.sh B5-reshard-up 4; v B5-up
bash scen-mem.sh after-up
bash scen-reshard.sh B5-reshard-down 3; v B5-down
bash scen-mem.sh after-down
bash scen-roll.sh B7-validity shd loader-shd "k patch kividbcluster shd --type merge -p '{\"spec\":{\"cluster\":{\"replicaValidityFactor\":0}}}'"; v B7
for p in $(k get pods -l app.kubernetes.io/instance=shd -o name | sed 's|pod/||'); do s=${p%-*}; echo "$p $(k exec cli -- redis-cli -h $p.$s-headless.e2e.svc -p 6380 --user default --pass $(k exec cli -- cat /tmp/pw-shd) --no-auth-warning config get cluster-replica-validity-factor | tail -1)"; done
k exec cli -- sh /tmp/repcheck.sh shd; sleep 8; k exec cli -- sh /tmp/repcheck.sh shd
bash scen-mem.sh after-B7
bash scen-gui.sh
bash scen-backup.sh
bash scen-reshard.sh B5b-reshard-up 4; v B5b-up
bash scen-reshard.sh B5b-reshard-down 3; v B5b-down
bash scen-mem.sh after-cycle2
echo SEQ_DONE
