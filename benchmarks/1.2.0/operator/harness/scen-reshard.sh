# scen-reshard.sh <scenario> <shards>: change spec.shards under load, wait for RebalanceCompleted + Running (+ removed pods gone), verify
source "$(dirname "$0")/env.sh"
SC=$1; NS=$2; n0=$(grep -c RebalanceCompleted "$D/events.log")
begin $SC; sleep 15
mark "$SC spec.shards=$NS"; k patch kividbcluster shd --type merge -p "{\"spec\":{\"shards\":$NS}}"
for i in $(seq 1 240); do [ "$(grep -c RebalanceCompleted "$D/events.log")" -gt "$n0" ] && break; sleep 5; done
mark "$SC RebalanceCompleted"
for i in $(seq 1 120); do np=$(k get pods -l app.kubernetes.io/instance=shd --no-headers | wc -l); ph=$(k get kividbcluster shd -o jsonpath='{.status.phase}'); [ "$np" = $((NS*2)) ] && [ "$ph" = Running ] && break; sleep 5; done
mark "$SC Running with $np pods"
k exec cli -- redis-cli -h shd -p 6380 --user default --pass $(k exec cli -- cat /tmp/pw-shd) --no-auth-warning cluster info | grep -E "cluster_state|cluster_size|known_nodes"
sleep 20; chk2 loader-shd $SC
masters; k get pvc --no-headers | grep -c "^data-shd"
