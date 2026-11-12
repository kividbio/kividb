# scen-dbops.sh <scenario> <cluster>: KividbDbOps restart (rolling restart, one handover per master), verify
source "$(dirname "$0")/env.sh"
SC=$1; C=$2; LDR=loader-shd; [ "$C" = rep ] && LDR=loader-rep
name=$(echo "$C-$SC-$(date +%H%M%S)" | tr 'A-Z' 'a-z')
begin $SC; sleep 15
mark "$SC DbOps restart"
printf 'apiVersion: kividb.io/v1alpha1\nkind: KividbDbOps\nmetadata: {name: %s, namespace: e2e}\nspec:\n  clusterRef: {name: %s}\n  op: restart\n' $name $C | k apply -f -
for i in $(seq 1 120); do p=$(k get kividbdbops $name -o jsonpath='{.status.phase}'); [ "$p" = Completed -o "$p" = Failed ] && break; sleep 5; done
mark "$SC DbOps $p"
sleep 20; chk2 $LDR $SC
