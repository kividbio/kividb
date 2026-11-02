cd "$(dirname "$0")"; source ./env.sh
bash scen-drain.sh drain-113 $(drainable_master)
bash scen-upgrade.sh upgrade-rep rep $ENG
bash scen-upgrade.sh upgrade-shd shd $ENG
for p in $(k get pods -l app.kubernetes.io/name=kividb -o name); do echo "$p $(k exec ${p#pod/} -c kividb -- kividb --version 2>/dev/null | head -1)"; done
bash scen-dbops.sh restart-rc4 shd
bash -c 'source ./env.sh; begin failover-rc4; sleep 15; rollfo failover-rc4; sleep 15; chk2 loader-shd failover-rc4'
bash scen-stop.sh sigstop-rc4 $(smaster 1)
bash scen-kill2.sh sigkill-rc4 $(smaster 0) inplace
bash scen-kill2.sh forcedel-rc4 $(smaster 2) force
bash scen-del.sh delete-rc4 $(smaster 1)
bash scen-drain.sh drain-rc4 $(drainable_master)
bash scen-reshard.sh reshard-up-rc4 4
bash scen-reshard.sh reshard-down-rc4 3
LD=loader-rep bash scen-stop.sh rep-sigstop-rc4 $(mp rep)
LD=loader-rep bash scen-kill2.sh rep-forcedel-rc4 $(mp rep) force
bash scen-dbops.sh rep-restart-rc4 rep
echo RC4_MAIN_DONE
bash -c 'source ./env.sh; bash scen-kill2.sh forcedel-rc4 $(smaster 2) force'
bash scen-repdel.sh repdel-rc4 0
bash scen-repdel.sh repdel2-rc4 2
bash scen-backup.sh
echo RC4_ALL_DONE
bash -c 'source ./env.sh; begin bulk-rc4; sleep 15; bash bulk.sh 2500000; sleep 30; chk2 loader-shd bulk-rc4'
bash scen-reshard.sh reshard-bulk-up-rc4 4
bash scen-reshard.sh reshard-bulk-down-rc4 3
for p in $(k get pods -l app.kubernetes.io/instance=shd -o name); do p=${p#pod/}; echo "$p $(k logs $p -c kividb --since=30m | grep -cE 'sent without it') replies-without-AOF, $(k logs $p -c kividb --since=30m | grep -c 'auto-checkpoint') checkpoints"; done
echo RC4_BULK_DONE
