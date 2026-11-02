# scen-backup.sh: (1) replicated backup + operator restore (bootstrapFromSnapshot) into a new cluster, compared key by key;
# (2) sharded backup (one run, all shards) + manual restore of one shard's archive, compared with that shard's master.
# Loaders are stopped while the copies are compared, so both sides hold the same keys.
source "$(dirname "$0")/env.sh"
E="$D/evidence/backup"; mkdir -p "$E"
PWR=$(k get secret rep-pw -o jsonpath='{.data.password}' | base64 -d); PWS=$(k get secret shd-pw -o jsonpath='{.data.password}' | base64 -d)
r() { k exec cli -- redis-cli -h $1 -p 6380 --user default --pass $2 --no-auth-warning "${@:3}"; }
mark "backup rep: loader stopped"; q loader-rep /stop
J=rep-bk-$(date +%H%M%S); k create job $J --from=cronjob/rep-backup; k wait --for=condition=complete job/$J --timeout=300s
k logs job/$J | tail -3 | tee "$E/$J.log"; mark "backup rep: $J complete"
M=$(mp rep); r $M.rep-headless.e2e.svc $PWR dbsize | tee "$E/rep-source-dbsize.txt"
k get kividbsnapshot -o wide | tee "$E/snapshots-rep.txt"
sed "s|IMAGE|$ENG|; s|SNAP|$J|" > "$D/rep-restored.yaml" <<'Y'
apiVersion: kividb.io/v1alpha1
kind: KividbCluster
metadata: {name: rep-restored, namespace: e2e}
spec:
  replicas: 1
  image: IMAGE
  storage: {size: 5Gi}
  aclConfigRef: {name: rep-acl}
  bootstrapFromSnapshot:
    snapshotRef: {name: SNAP}
Y
t0=$(date +%s); k apply -f "$D/rep-restored.yaml"
for i in $(seq 1 60); do [ "$(k get kividbcluster rep-restored -o jsonpath='{.status.phase}')" = Running ] && break; sleep 5; done
mark "backup rep: rep-restored Running after $(( $(date +%s) - t0 )) s"
for p in rep-restored-0 rep-restored-1; do echo "$p $(r $p.rep-restored-headless.e2e.svc $PWR dbsize)"; done | tee "$E/rep-restored-dbsize.txt"
k exec cli -- sh -c "redis-cli -h $M.rep-headless.e2e.svc -p 6380 --user default --pass $PWR --no-auth-warning --scan --count 1000 | sort > /tmp/a; redis-cli -h rep-restored-0.rep-restored-headless.e2e.svc -p 6380 --user default --pass $PWR --no-auth-warning --scan --count 1000 | sort > /tmp/b; wc -l < /tmp/a; wc -l < /tmp/b; cmp /tmp/a /tmp/b && echo KEYLISTS_IDENTICAL; shuf -n 300 /tmp/a > /tmp/s; bad=0; for key in \$(cat /tmp/s); do x=\$(redis-cli -h $M.rep-headless.e2e.svc -p 6380 --user default --pass $PWR --no-auth-warning get \$key); y=\$(redis-cli -h rep-restored-0.rep-restored-headless.e2e.svc -p 6380 --user default --pass $PWR --no-auth-warning get \$key); [ \"\$x\" = \"\$y\" ] || bad=\$((bad+1)); done; echo sampled 300 values, mismatches \$bad" | tee "$E/rep-compare.txt"
q loader-rep /start; mark "backup rep: loader started"

mark "backup shd: loader stopped"; q loader-shd /stop
J=shd-bk-$(date +%H%M%S); k create job $J --from=cronjob/shd-backup; k wait --for=condition=complete job/$J --timeout=600s
k logs job/$J | tail -3 | tee "$E/$J.log"; mark "backup shd: $J complete"
k get kividbsnapshot $J -o yaml | sed -n '/^status:/,$p' | tee "$E/$J-status.yaml"
SM=$(smaster 2); r $SM.shd-2-headless.e2e.svc $PWS dbsize | tee "$E/shd-2-master-dbsize.txt"
MAN=$(k get kividbsnapshot $J -o jsonpath='{.status.objectKey}'); SRC=$(k get kividbsnapshot $J -o jsonpath='{.status.shards[?(@.index==2)].sourcePod}'); OBJ="${MAN%/*}/shard-2-$SRC.tar.gz"
echo "shard 2 object: $OBJ" | tee -a "$E/shd-2-master-dbsize.txt"
k delete pod shard2-restore --ignore-not-found --wait=true
sed "s|IMAGE|$ENG|; s|OBJ|$OBJ|" > "$D/shard2-restore.yaml" <<'Y'
apiVersion: v1
kind: Pod
metadata: {name: shard2-restore, namespace: e2e}
spec:
  nodeSelector: {e2e-infra: "true"}
  initContainers:
    - name: fetch
      image: curlimages/curl:8.11.1
      command: ["sh","-c","cd /data && curl -sf http://s3.e2e.svc:9000/backups/OBJ -o s.tgz && tar xzf s.tgz && rm s.tgz && ls -la /data && rm -f nodes.conf *.imports; chmod 666 *; true"]
      volumeMounts: [{name: data, mountPath: /data}]
  containers:
    - name: kividb
      image: IMAGE
      args: ["--dir","/data","--protected-mode","no","--aof"]
      volumeMounts: [{name: data, mountPath: /data}]
  volumes: [{name: data, emptyDir: {}}]
Y
k apply -f "$D/shard2-restore.yaml"; k wait --for=condition=Ready pod/shard2-restore --timeout=300s; sleep 5
k logs shard2-restore -c fetch | tail -5 | tee "$E/shard2-restore-fetch.log"; k logs shard2-restore -c kividb | grep -E "Loaded|AOF|keys" | tee "$E/shard2-restore.log"
RIP=$(k get pod shard2-restore -o jsonpath='{.status.podIP}')
k exec cli -- sh -c "echo restored dbsize \$(redis-cli -h $RIP -p 6380 dbsize); redis-cli -h $RIP -p 6380 --scan --count 1000 | shuf -n 300 > /tmp/s; bad=0; for key in \$(cat /tmp/s); do x=\$(redis-cli -c -h shd -p 6380 --user default --pass $PWS --no-auth-warning get \$key); y=\$(redis-cli -h $RIP -p 6380 get \$key); [ \"\$x\" = \"\$y\" ] || bad=\$((bad+1)); done; echo sampled 300 values vs live cluster, mismatches \$bad; for key in \$(head -50 /tmp/s); do redis-cli -h $RIP -p 6380 cluster keyslot \$key; done | sort -n | sed -n '1p;\$p'" | tee "$E/shard2-compare.txt"
q loader-shd /start; mark "backup shd: loader started"
k delete pod shard2-restore --wait=false
echo BACKUP_DONE
