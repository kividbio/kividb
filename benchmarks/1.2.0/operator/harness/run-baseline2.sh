cd "$(dirname "$0")"; source ./env.sh
bash scen-reshard.sh reshard-down-113 3
bash scen-dbops.sh restart-113 shd
bash -c 'source ./env.sh; begin failover-113; sleep 15; rollfo failover-113; sleep 15; chk2 loader-shd failover-113'
bash scen-stop.sh sigstop-113 $(smaster 1)
bash scen-kill2.sh sigkill-113 $(smaster 0) inplace
bash scen-kill2.sh forcedel-113 $(smaster 2) force
bash scen-del.sh delete-113 $(smaster 1)
bash scen-drain.sh drain-113 $(smaster 0)
LD=loader-rep bash scen-stop.sh rep-sigstop-113 $(mp rep)
LD=loader-rep bash scen-kill2.sh rep-forcedel-113 $(mp rep) force
echo BASELINE_DONE
