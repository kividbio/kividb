# run-c3-rep.sh: the replicated track (rep, 1 master + 2 replicas, from v1.2.0), run beside run-c3-shd.sh.
cd "$(dirname "$0")"; source ./env.sh
export PW=pw-one-1111
setpw() { k create secret generic rep-pw --from-literal=password=$1 --dry-run=client -o yaml | k apply -f - ; q loader-rep "/password?p=$1"; k exec cli -- sh -c "echo $1 > /tmp/pw"; }
export -f setpw
echo "=== rep before A1"; rinfo rep
bash scen-upgrade.sh A1-upgrade rep $ENG
echo "=== rep after A1"; rinfo rep
LD=loader-rep bash scen-del.sh A2a-delete $(mp rep)
for n in 1 2 3; do bash scen-dbops.sh A3-restart-$n rep; done
bash scen-roll.sh A4a-password rep loader-rep setpw pw-two-2222; export PW=pw-two-2222
bash scen-roll.sh A4b-nopassword rep loader-rep "k patch kividbaclconfig rep-acl --type json -p '[{\"op\":\"remove\",\"path\":\"/spec/requirePassSecretRef\"}]'"
bash scen-roll.sh A4c-password rep loader-rep "setpw pw-three-3333; k patch kividbaclconfig rep-acl --type merge -p '{\"spec\":{\"requirePassSecretRef\":{\"name\":\"rep-pw\",\"key\":\"password\"}}}'"; export PW=pw-three-3333
for p in rep-0 rep-1 rep-2; do echo "$p old pw-two: $(k exec cli -- redis-cli -h $p.rep-headless.e2e.svc -p 6380 --user default --pass pw-two-2222 --no-auth-warning ping 2>&1 | head -1) new: $(rc $p.rep-headless.e2e.svc ping | head -1)"; done
bash scen-c1.sh
LD=loader-rep bash scen-kill2.sh A2b-forcedel $(mp rep) force
LD=loader-rep bash scen-kill2.sh A8-sigkill $(mp rep) inplace
echo "=== rep end"; rinfo rep
echo REP_TRACK_DONE
