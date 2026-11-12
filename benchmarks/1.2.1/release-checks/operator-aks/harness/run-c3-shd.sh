# run-c3-shd.sh: the sharded track (shd, 3 x (1+1), formed on v1.2.0), run beside run-c3-rep.sh.
cd "$(dirname "$0")"; source ./env.sh
v() { echo "--- views after $1 $(ts)"; views; }
setspw() { k create secret generic shd-pw --from-literal=password=$1 --dry-run=client -o yaml | k apply -f - ; q loader-shd "/password?p=$1"; k exec cli -- sh -c "echo $1 > /tmp/pw-shd"; }
export -f setspw
echo "=== B1 formed"; k exec cli -- redis-cli -h shd -p 6380 --user default --pass $(k exec cli -- cat /tmp/pw-shd) --no-auth-warning cluster info | grep -E "cluster_state|slots_assigned|known_nodes|cluster_size"; masters; k exec cli -- sh /tmp/repcheck.sh shd
bash scen-upgrade.sh U2-upgrade shd $ENG; v U2
bash scen-del.sh Bdel-delete $(smaster 1); v Bdel
bash scen-kill2.sh B2-forcedel $(smaster 2) force; v B2
bash scen-kill2.sh B2k-sigkill $(smaster 0) inplace; v B2k
bash scen-stop.sh B2a-sigstop $(smaster 1); v B2a
bash scen-roll.sh B3-roll shd loader-shd "k patch kividbcluster shd --type merge -p '{\"spec\":{\"podAnnotations\":{\"e2e/roll\":\"b3\"}}}'"; v B3
bash scen-dbops.sh B4-restart shd; v B4
bash scen-roll.sh Bpw-password shd loader-shd setspw shd-pw-2222; v Bpw
for p in shd-0-0 shd-1-0 shd-2-0; do s=${p%-*}; echo "$p old: $(k exec cli -- redis-cli -h $p.$s-headless.e2e.svc -p 6380 --user default --pass shd-pw-1111 --no-auth-warning ping 2>&1 | head -1) new: $(k exec cli -- redis-cli -h $p.$s-headless.e2e.svc -p 6380 --user default --pass shd-pw-2222 --no-auth-warning ping 2>&1 | head -1)"; done
k exec cli -- sh /tmp/repcheck.sh shd
echo SHD_TRACK_DONE
