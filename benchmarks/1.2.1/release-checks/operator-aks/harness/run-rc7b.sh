# run-rc7b.sh: the operator fixes on master/#42 and the #42 AOF-master guard, on rc7; then backup and restore.
cd "$(dirname "$0")"; source ./env.sh
v() { echo "--- views after $1 $(ts)"; views; }
bash scen-dbq.sh dbq-rc7 rep
bash scen-holdback.sh holdback-rc7 rep
bash scen-fblock.sh fblock-rc7 fb
bash scen-guard.sh guard-yes g-yes loader-g-yes opdown
bash scen-guard.sh guard-no g-no loader-g-no opdown
bash scen-guard.sh guard-113 g-113 loader-g-113 opdown
bash scen-guard.sh guard-bad g-bad loader-g-bad opdown
bash scen-guard.sh guard-yes-opup g-yes loader-g-yes opup
echo GUARD_DONE
bash scen-shrink.sh shrink-a-rc7 a; v shrink-a-rc7
bash scen-shrink.sh shrink-b-rc7 b; v shrink-b-rc7
bash scen-reshard.sh reshard-down2-rc7 3; v reshard-down2-rc7
bash scen-backup.sh
bash scen-bootguard.sh bootguard-rc7
echo RC7B_DONE
