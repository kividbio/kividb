# the rest of the rc4 run, after delete-rc4 (which was stopped by the split cluster view); views after every scenario
cd "$(dirname "$0")"; source ./env.sh
v() { echo "--- views after $1 $(ts)"; views; } 
bash scen-drain.sh drain-rc4 $(drainable_master); v drain-rc4
bash scen-reshard.sh reshard-up-rc4 4; v reshard-up-rc4
bash scen-reshard.sh reshard-down-rc4 3; v reshard-down-rc4
bash scen-kill2.sh forcedel-rc4 $(smaster 2) force; v forcedel-rc4
bash scen-repdel.sh repdel-rc4 0; v repdel-rc4
bash scen-repdel.sh repdel2-rc4 2; v repdel2-rc4
LD=loader-rep bash scen-stop.sh rep-sigstop-rc4 $(mp rep)
LD=loader-rep bash scen-kill2.sh rep-forcedel-rc4 $(mp rep) force
bash scen-dbops.sh rep-restart-rc4 rep
echo RC4_MAIN_DONE
bash scen-backup.sh
echo BACKUP_DONE
bash -c 'source ./env.sh; begin bulk-rc4; sleep 15; bash bulk.sh 2500000; sleep 30; chk2 loader-shd bulk-rc4'
bash scen-reshard.sh reshard-bulk-up-rc4 4; v reshard-bulk-up-rc4
bash scen-reshard.sh reshard-bulk-down-rc4 3; v reshard-bulk-down-rc4
for p in $(k get pods -l app.kubernetes.io/instance=shd -o name); do p=${p#pod/}; echo "$p $(k logs $p -c kividb --since=30m | grep -cE 'sent without it') replies-without-AOF, $(k logs $p -c kividb --since=30m | grep -c 'auto-checkpoint') checkpoints"; done
echo RC4_BULK_DONE
bash scen-split.sh split-repro-rc4
echo SPLIT_DONE
