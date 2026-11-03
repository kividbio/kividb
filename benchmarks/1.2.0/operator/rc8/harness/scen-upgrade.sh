# scen-upgrade.sh <scenario> <cluster> <image>: change spec.image under load, wait until every pod runs it and is Ready, verify (LD=<loader> to choose the loader)
source "$(dirname "$0")/env.sh"
SC=$1; C=$2; IMG=$3; LDR=loader-shd; [ "$C" = rep ] && LDR=loader-rep; LDR=${LD:-$LDR}
n=$(k get pods -l app.kubernetes.io/instance=$C --no-headers | wc -l)
begin $SC; sleep 15
mark "$SC spec.image -> ${IMG##*:}"; k patch kividbcluster $C --type merge -p "{\"spec\":{\"image\":\"$IMG\"}}"
for i in $(seq 1 180); do
  ok=$(k get pods -l app.kubernetes.io/instance=$C -o jsonpath='{range .items[*]}{.spec.containers[?(@.name=="kividb")].image}|{.status.containerStatuses[?(@.name=="kividb")].ready}{"\n"}{end}' | grep -c "^$IMG|true")
  ph=$(k get kividbcluster $C -o jsonpath='{.status.phase}')
  [ "$ok" = "$n" ] && [ "$ph" = Running ] && break; sleep 5; done
mark "$SC all $ok pods on ${IMG##*:}, $ph"
sleep 20; chk2 $LDR $SC
