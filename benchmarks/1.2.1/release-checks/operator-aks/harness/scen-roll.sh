# scen-roll.sh <scenario> <cluster> <loader> <command...>: run a command that makes the operator roll every pod
# (spec change, Secret edit, ACL change), wait until every pod of the cluster has been replaced, is Ready and the
# cluster is Running again, then verify the loader. Used for B3, B7, A4a/b/c, B-pw. (c3 run)
source "$(dirname "$0")/env.sh"
SC=$1; C=$2; LDR=$3; shift 3
uids0=$(k get pods -l app.kubernetes.io/instance=$C -o jsonpath='{range .items[*]}{.metadata.uid}{"\n"}{end}' | sort)
n=$(echo "$uids0" | wc -l)
begin $SC; sleep 15
mark "$SC $*"; eval "$@"
for i in $(seq 1 180); do
  cur=$(k get pods -l app.kubernetes.io/instance=$C -o jsonpath='{range .items[*]}{.metadata.uid}|{.status.containerStatuses[?(@.name=="kividb")].ready}{"\n"}{end}')
  new=$(echo "$cur" | grep '|true$' | cut -d'|' -f1 | sort | comm -23 - <(echo "$uids0") | wc -l)
  ph=$(k get kividbcluster $C -o jsonpath='{.status.phase}')
  [ "$new" -ge "$n" ] && [ "$ph" = Running ] && break; sleep 5; done
mark "$SC $new/$n pods replaced, $ph"
sleep 20; chk2 $LDR $SC
