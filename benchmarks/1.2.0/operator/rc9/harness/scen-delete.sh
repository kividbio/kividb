# scen-delete.sh <scenario> <pod> <force|graceful>: delete a shard master of shd (rc8's scen-fxrepro.sh, on shd) while
#  - a watcher on its node lists, every 100 ms for 75 s, the kividb processes whose HOSTNAME is the pod (two at once =
#    two engines on one volume) and the data directory's files (name, size, mtime) through /proc/<pid>/root,
#  - its log is followed until the old engine exits (handover, shutdown wait, final save),
# then the new engine's log, the shard's key counts, and every acknowledged write of loader-shd read back.
# SAMENODE=1 cordons every other node for the moment of the delete, so the replacement lands on the same node.
source "$(dirname "$0")/env.sh"
SC=$1; P=$2; HOW=$3; LDR=${LD:-loader-shd}; E="$D/evidence/$SC"; mkdir -p "$E"; uid0=$(k get pod $P -o jsonpath='{.metadata.uid}')
S=${P%-*}; PEER=$S-$(( 1 - ${P##*-} ))
N=$(k get pod $P -o jsonpath='{.spec.nodeName}'); ND=$(k get pod -l app=nd -o jsonpath="{.items[?(@.spec.nodeName=='$N')].metadata.name}")
keys() { k exec cli -- sh -c "for h in $P $PEER; do echo \$h \$(redis-cli -h \$h.$S-headless.e2e.svc -p 6380 --user default --pass \$(cat /tmp/pw-shd) --no-auth-warning dbsize) \$(redis-cli -h \$h.$S-headless.e2e.svc -p 6380 --user default --pass \$(cat /tmp/pw-shd) --no-auth-warning role | head -1); done"; }
keys > "$E/before.txt"
k logs -f $P -c kividb --since=1s --timestamps > "$E/$P-old.log" 2>&1 &
k logs -f $PEER -c kividb --since=1s --timestamps > "$E/$PEER.log" 2>&1 &
LP=$!
k exec $ND -- sh -c "end=\$((\$(date +%s)+75)); while [ \$(date +%s) -lt \$end ]; do t=\$(date -u +%H:%M:%S.%N | cut -c1-12); l=''; for p in \$(pidof kividb); do tr '\0' '\n' < /proc/\$p/environ 2>/dev/null | grep -qx HOSTNAME=$P && { l=\"\$l \$p\"; f=\$(ls -l --time-style=+%H:%M:%S.%N /proc/\$p/root/data/ 2>/dev/null | awk 'NR>1{print \$NF\":\"\$5\":\"\$6}' | tr '\n' ' '); l=\"\$l[\$f]\"; }; done; echo \"\$t pids:\$l\"; sleep 0.1; done" > "$E/procs.txt" 2>&1 &
W=$!
begin $SC; sleep 10
OTHERS=""; [ -n "$SAMENODE" ] && { OTHERS=$(kubectl get nodes -o name | grep -v "$N"); kubectl cordon $OTHERS >/dev/null; mark "$SC cordoned all nodes but ${N##*-}"; }
if [ "$HOW" = force ]; then mark "$SC force delete $P"; k delete pod $P --grace-period=0 --force 2>&1 | tail -1
else mark "$SC graceful delete $P"; k delete pod $P --wait=false; fi
for i in $(seq 1 120); do u=$(k get pod $P -o jsonpath='{.metadata.uid}' 2>/dev/null); [ -n "$u" ] && [ "$u" != "$uid0" ] && break; sleep 1; done
[ -n "$OTHERS" ] && { for i in $(seq 1 60); do [ -n "$(k get pod $P -o jsonpath='{.spec.nodeName}')" ] && break; sleep 0.5; done; kubectl uncordon $OTHERS >/dev/null; mark "$SC new $P on $(k get pod $P -o jsonpath='{.spec.nodeName}' | sed 's/.*-//'), nodes uncordoned"; }
waitready $P 240; mark "$SC $P Ready"; wait $W
sleep 5; k logs $P -c kividb --timestamps > "$E/$P-new.log" 2>&1; kill $LP 2>/dev/null
keys > "$E/after.txt"
awk '{n=split($0,a," pids:"); c=split(a[2],b," "); x=0; for(i=1;i<=c;i++) if (b[i] ~ /^[0-9]+/) x++; if (x>1) print}' "$E/procs.txt" | head -5 > "$E/overlap.txt"
# dump.kdb as the watcher saw it: one line per change of size or mtime
grep -oE 'dump\.kdb:[0-9]+:[0-9:.]+' "$E/procs.txt" | uniq > "$E/dump-sizes.txt"
{ echo "lines with two engines for $P: $(awk '{split($0,a," pids:"); c=split(a[2],b," "); x=0; for(i=1;i<=c;i++) if (b[i] ~ /^[0-9]+/) x++; if (x>1) n++} END {print n+0}' "$E/procs.txt")"
  echo "keys before: $(tr '\n' ';' < "$E/before.txt")  after: $(tr '\n' ';' < "$E/after.txt")"
  echo "dump.kdb sizes seen: $(cut -d: -f2 "$E/dump-sizes.txt" | uniq | tr '\n' ' ')"
  grep -hE "in use by another|is free after|Found dump|Loaded [0-9]+ keys|Replaying|AOF replay|saved|Saving|final snapshot|final save|Shutdown|SIGTERM|rebase|Goodbye|Partial resync|Full resync|whole stream|did not acknowledge|still being sent|dataset is offset|continues|Manual failover|Failover|promot" "$E/$P-old.log" "$E/$P-new.log" | cut -c1-260; } > "$E/summary.txt"
sleep 15; chk2 $LDR $SC
