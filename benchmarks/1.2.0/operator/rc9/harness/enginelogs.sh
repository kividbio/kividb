# enginelogs.sh: follow every engine pod's log (by name, re-attached when the pod is replaced) into $D/elogs/<pod>.log,
# with Kubernetes timestamps, for the whole run; synccount.py counts each scenario's resyncs and shutdown waits from them.
source "$(dirname "$0")/env.sh"; mkdir -p "$D/elogs"
for p in shd-0-0 shd-0-1 shd-1-0 shd-1-1 shd-2-0 shd-2-1 shd-3-0 shd-3-1 g-yes-0 g-yes-1 g-no-0 g-no-1; do
  ( while true; do kubectl -n e2e logs -f $p -c kividb --timestamps --since=15s 2>/dev/null; sleep 3; done >> "$D/elogs/$p.log" ) &
done
wait
