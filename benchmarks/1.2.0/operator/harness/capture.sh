source "$(dirname "$0")/env.sh"
( while true; do kubectl -n e2e get events -w --output-watch-events=false -o custom-columns=T:.lastTimestamp,TYPE:.type,R:.reason,O:.involvedObject.name,M:.message --no-headers; sleep 2; done >> "$D/events.log" 2>&1 ) &
( while true; do kubectl -n kividb-system logs -f deploy/kividb-operator-manager --since=5s; sleep 2; done >> "$D/operator.log" 2>&1 ) &
( while true; do kubectl -n e2e get pods -w -o wide --no-headers | while read l; do echo "$(date -u +%H:%M:%S) $l"; done; sleep 2; done >> "$D/pods-watch.log" 2>&1 ) &
wait
