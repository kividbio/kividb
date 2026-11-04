# run-rc9.sh: the short rc9 operator pass, in order. Start state: shd (3 x (1+1)) on rc8, g-yes and g-no on rc9, their
# loaders writing; enginelogs.sh and capture.sh running.
#  1. upgrade rc8 -> rc9 (sharded);
#  2. rolling restart (KividbDbOps);
#  3. CLUSTER FAILOVER of every shard, with sync counters and each demoted master's log;
#  4. 2 graceful deletes of a shard master, then 3 same-node force deletes (#303);
#  5. kill -9 of a shard master;
#  6. reshard 3 -> 4 -> 3;
#  7. the #42 guard: rc9 with aof-ack-after-write yes (master kept) and no (failover).
# "views" (every node's view of every peer) and the sync counters after each sharded scenario.
cd "$(dirname "$0")"; source ./env.sh
v() { echo "--- views after $1 $(ts)"; views; syncs "after $1" >/dev/null; }
vers() { for p in $(k get pods -l app.kubernetes.io/name=kividb -o name); do echo "$p $(k exec ${p#pod/} -c kividb -- kividb --version 2>/dev/null | head -1)"; done; }
syncs start >/dev/null
bash scen-upgrade.sh upgrade-shd shd $ENG; vers; v upgrade-shd
bash scen-dbops.sh restart-rc9 shd; v restart-rc9
bash scen-failover.sh failover-rc9; v failover-rc9
bash run-deletes.sh; v deletes
bash scen-kill2.sh sigkill-rc9 $(smaster 0) inplace; v sigkill-rc9
bash scen-reshard.sh reshard-up-rc9 4; v reshard-up-rc9
bash scen-reshard.sh reshard-down-rc9 3; v reshard-down-rc9
vers
bash scen-guard.sh guard-yes g-yes loader-g-yes opdown
bash scen-guard.sh guard-no g-no loader-g-no opdown
echo RC9_DONE
