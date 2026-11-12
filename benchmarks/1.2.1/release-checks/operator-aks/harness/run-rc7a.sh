# run-rc7a.sh: the rc4 run's scenarios on rc7: engine upgrade 1.1.3 -> rc7 of both clusters, then the sharded and
# replicated failure scenarios, the split repro, resharding. "views" (every node's view of every peer) after each.
cd "$(dirname "$0")"; source ./env.sh
v() { echo "--- views after $1 $(ts)"; views; }
bash scen-upgrade.sh upgrade-rep rep $ENG
bash scen-upgrade.sh upgrade-shd shd $ENG
for p in $(k get pods -l app.kubernetes.io/name=kividb -o name); do echo "$p $(k exec ${p#pod/} -c kividb -- kividb --version 2>/dev/null | head -1)"; done
v upgrade-shd
bash scen-dbops.sh restart-rc7 shd; v restart-rc7
bash -c 'source ./env.sh; begin failover-rc7; sleep 15; rollfo failover-rc7; sleep 15; chk2 loader-shd failover-rc7'; v failover-rc7
bash scen-stop.sh sigstop-rc7 $(smaster 1); v sigstop-rc7
bash scen-kill2.sh sigkill-rc7 $(smaster 0) inplace; v sigkill-rc7
bash scen-kill2.sh forcedel-rc7 $(smaster 2) force; v forcedel-rc7
bash scen-del.sh delete-rc7 $(smaster 1); v delete-rc7
bash scen-drain.sh drain-rc7 $(drainable_master); v drain-rc7
bash scen-split.sh split-rc7; v split-rc7
bash scen-repdel.sh repdel-rc7 0; v repdel-rc7
bash scen-reshard.sh reshard-up-rc7 4; v reshard-up-rc7
bash scen-reshard.sh reshard-down-rc7 3; v reshard-down-rc7
LD=loader-rep bash scen-stop.sh rep-sigstop-rc7 $(mp rep)
LD=loader-rep bash scen-kill2.sh rep-sigkill-rc7 $(mp rep) inplace
LD=loader-rep bash scen-kill2.sh rep-forcedel-rc7 $(mp rep) force
bash scen-dbops.sh rep-restart-rc7 rep
echo RC7A_DONE
