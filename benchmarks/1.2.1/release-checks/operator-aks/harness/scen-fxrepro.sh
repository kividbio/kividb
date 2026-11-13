# scen-fxrepro.sh <scenario> <pod> <force|graceful>: delete a shard master of cluster fx while
#  - a watcher on its node lists, every 100 ms, the kividb processes whose HOSTNAME is the pod (two at once = two
#    engines on one volume),
#  - its log is followed until the old engine exits (final dump, point file),
#  - the data directory's files (name, size, mtime in ns) are listed every 100 ms through /proc/<pid>/root;
# then the new engine's log, and every acknowledged write of loader-fx read back.
source "$(dirname "$0")/env.sh"
SC=$1; P=$2; HOW=$3; E="$D/evidence/$SC"; mkdir -p "$E"; uid0=$(k get pod $P -o jsonpath='{.metadata.uid}')
N=$(k get pod $P -o jsonpath='{.spec.nodeName}'); ND=$(k get pod -l app=nd -o jsonpath="{.items[?(@.spec.nodeName=='$N')].metadata.name}")
k logs -f $P -c kividb --since=1s --timestamps > "$E/$P-old.log" 2>&1 &
k exec $ND -- sh -c "end=\$((\$(date +%s)+40)); while [ \$(date +%s) -lt \$end ]; do t=\$(date -u +%H:%M:%S.%N | cut -c1-12); l=''; for p in \$(pidof kividb); do tr '\0' '\n' < /proc/\$p/environ 2>/dev/null | grep -qx HOSTNAME=$P && { l=\"\$l \$p\"; f=\$(ls -l --time-style=+%H:%M:%S.%N /proc/\$p/root/data/ 2>/dev/null | awk 'NR>1{print \$NF\":\"\$5\":\"\$6}' | tr '\n' ' '); l=\"\$l[\$f]\"; }; done; echo \"\$t pids:\$l\"; sleep 0.1; done" > "$E/procs.txt" 2>&1 &
W=$!
begin $SC; sleep 10
# SAMENODE=1: cordon every other node for the moment of the delete, so the replacement lands on the same node and
# mounts the volume there at once (what happened in forcedel-rc7); uncordoned as soon as the new pod is scheduled
OTHERS=""; [ -n "$SAMENODE" ] && { OTHERS=$(kubectl get nodes -o name | grep -v "$N"); kubectl cordon $OTHERS >/dev/null; mark "$SC cordoned all nodes but ${N##*-}"; }
if [ "$HOW" = force ]; then mark "$SC force delete $P"; k delete pod $P --grace-period=0 --force 2>&1 | tail -1
else mark "$SC graceful delete $P"; k delete pod $P --wait=false; fi
for i in $(seq 1 90); do u=$(k get pod $P -o jsonpath='{.metadata.uid}' 2>/dev/null); [ -n "$u" ] && [ "$u" != "$uid0" ] && break; sleep 1; done
[ -n "$OTHERS" ] && { for i in $(seq 1 60); do [ -n "$(k get pod $P -o jsonpath='{.spec.nodeName}')" ] && break; sleep 0.5; done; kubectl uncordon $OTHERS >/dev/null; mark "$SC new $P on $(k get pod $P -o jsonpath='{.spec.nodeName}' | sed 's/.*-//'), nodes uncordoned"; }
waitready $P 240; mark "$SC $P Ready"; wait $W
k logs $P -c kividb --timestamps > "$E/$P-new.log" 2>&1
awk '{n=split($0,a," pids:"); c=split(a[2],b," "); x=0; for(i=1;i<=c;i++) if (b[i] ~ /^[0-9]+/) x++; if (x>1) print}' "$E/procs.txt" | head -5 > "$E/overlap.txt"
echo "lines with two engines for $P: $(awk '{split($0,a," pids:"); c=split(a[2],b," "); x=0; for(i=1;i<=c;i++) if (b[i] ~ /^[0-9]+/) x++; if (x>1) n++} END {print n+0}' "$E/procs.txt")" | tee "$E/summary.txt"
grep -hE "Found dump|Loaded [0-9]+ keys|Replaying|AOF replay|Loaded dataset|offset|saved|Saving|DB saved|snapshot|shutdown|Shutdown|SIGTERM|signal|rebase|exit" "$E/$P-old.log" "$E/$P-new.log" | cut -c1-240 >> "$E/summary.txt"
sleep 20; chk2 loader-fx $SC
