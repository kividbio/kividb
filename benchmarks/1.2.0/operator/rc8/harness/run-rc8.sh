# run-rc8.sh: the rc8 operator pass, in order. Start state: rep and shd on 1.1.3, fx on rc7, all three loaders writing.
#  1. upgrades: rep and shd 1.1.3 -> rc8, fx rc7 -> rc8;
#  2. the #303 force-delete repro on fx (run-fxloss.sh: 6 same-node force deletes + 1 graceful), then fx removed and the
#     guard clusters created (they load while 3 runs);
#  3. the rc7 scenarios on shd: rolling restart (DbOps), CLUSTER FAILOVER of every shard, kill -9 of a shard master,
#     node drain, reshard 3 -> 4 -> 3;
#  4. the #42 AOF-master guard: rc8 yes, rc8 no, 1.1.3;
#  5. backup and restore.
# "views" (every node's view of every peer) after each sharded scenario.
cd "$(dirname "$0")"; source ./env.sh
v() { echo "--- views after $1 $(ts)"; views; }
vers() { for p in $(k get pods -l app.kubernetes.io/name=kividb -o name); do echo "$p $(k exec ${p#pod/} -c kividb -- kividb --version 2>/dev/null | head -1)"; done; }
bash scen-upgrade.sh upgrade-rep rep $ENG
bash scen-upgrade.sh upgrade-shd shd $ENG
LD=loader-fx bash scen-upgrade.sh upgrade-fx fx $ENG
vers; v upgrade-shd
bash run-fxloss.sh 6
mark "fx removed"; q loader-fx /stats > "$D/res/fx-final-stats.json"; q loader-fx /stop >/dev/null; q loader-fx /verify > "$D/res/fx-final-verify.json"
k delete deploy loader-fx; k delete kividbcluster fx --wait=true; k delete $(k get pvc -o name | grep '/data-fx-') --wait=false
sed "s/ACR/${A%%.*}/" guard.yaml | k apply -f -; sleep 60; sed "s/ACR/${A%%.*}/" loader-guard.yaml | k apply -f -; mark "guard clusters and loaders created"
bash scen-dbops.sh restart-rc8 shd; v restart-rc8
bash -c 'source ./env.sh; begin failover-rc8; sleep 15; rollfo failover-rc8; sleep 15; chk2 loader-shd failover-rc8'; v failover-rc8
bash scen-kill2.sh sigkill-rc8 $(smaster 0) inplace; v sigkill-rc8
bash scen-drain.sh drain-rc8 $(drainable_master); v drain-rc8
bash scen-reshard.sh reshard-up-rc8 4; v reshard-up-rc8
bash scen-reshard.sh reshard-down-rc8 3; v reshard-down-rc8
vers
bash scen-guard.sh guard-yes g-yes loader-g-yes opdown
bash scen-guard.sh guard-no g-no loader-g-no opdown
bash scen-guard.sh guard-113 g-113 loader-g-113 opdown
echo GUARD_DONE
bash scen-backup.sh
echo RC8_DONE
