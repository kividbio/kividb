#!/usr/bin/env bash
# sen_run.sh <engine> <scenario> <rep>   (runs on the client VM)
# Sentinel failover: master on FO1, replicas on FO2 and FO3, three redis-sentinel 8.10.3 processes on this
# host (ports 26379-26381, quorum 2, down-after 5000 ms, failover-timeout 60000 ms). Writers find the master
# through Sentinel (ackcheck.py --mode sentinel: redis-py Sentinel.master_for), log every acked write and read
# them all back from the master Sentinel names at the end.
#   sen_kill9      kill -9 the master at t=20; restart it at t=50 (Sentinel must make it a replica)
#   sen_manual     SENTINEL FAILOVER mymaster at t=20
#   sen_prio0      FO2 has replica-priority 0: kill -9 the master; FO3 must be promoted, never FO2
#   sen_prio0_all  both replicas replica-priority 0: kill -9 the master; nothing may be promoted
set -u
H=$(cd "$(dirname "$0")" && pwd)
[ -f "$H/hosts.env" ] && . "$H/hosts.env"
E=$1 S=$2 REP=${3:-1}
RES=${RES:-$HOME/sen-results}
OUT=$RES/$E/$S/$REP
rm -rf "$OUT"; mkdir -p "$OUT"
PY=$HOME/venv/bin/python
EVF=$OUT/events; : > "$EVF"
ME=${ME:-10.92.1.10}
SPORTS="26379 26380 26381"
SENTS="$ME:26379,$ME:26380,$ME:26381"
n()  { local h=$1; shift; ssh -o StrictHostKeyChecking=no -o LogLevel=ERROR "$h" "bash node.sh $(printf '%q ' "$@")"; }
rc() { local hp=$1; shift; redis-cli -h "${hp%:*}" -p "${hp#*:}" "$@" 2>&1; }
ev() { echo "$(date +%s.%N) $*" >> "$EVF"; echo "  [$(date +%T)] $*"; }
info() { rc "$1" INFO "$2" | tr -d '\r' | awk -F: -v k="$3" '$1==k{print $2; exit}'; }
at() { local t0; t0=$(cat "$OUT/t0" 2>/dev/null || true)
  while [ -z "$t0" ]; do sleep 0.05; t0=$(cat "$OUT/t0" 2>/dev/null || true); done
  local d; d=$(awk -v a="$t0" -v b="$1" -v c="$(date +%s.%N)" 'BEGIN{x=a+b-c; printf "%.3f", (x>0?x:0)}'); sleep "$d"; }
smaster() { redis-cli -p 26379 SENTINEL get-master-addr-by-name mymaster 2>/dev/null | tr '\n' ':' | sed 's/:$//'; }

sentinels_stop() { pkill -9 -f '[r]edis-server.*:263(79|80|81)' 2>/dev/null; sleep 0.5; }
sentinels_start() {
  sentinels_stop
  for p in $SPORTS; do
    d=/data/s$p; rm -rf $d; mkdir -p $d
    cat > $d/sentinel.conf <<EOF
port $p
bind 0.0.0.0
protected-mode no
dir $d
logfile $d/sentinel.log
sentinel announce-ip $ME
sentinel monitor mymaster $FO1 6379 2
sentinel down-after-milliseconds mymaster 5000
sentinel failover-timeout mymaster 60000
sentinel parallel-syncs mymaster 1
EOF
    (cd $d && exec /opt/redis/redis-server $d/sentinel.conf --sentinel) > /dev/null 2>&1 < /dev/null &
  done
  # all three see each other and both replicas
  for _ in $(seq 1 120); do
    ok=0
    for p in $SPORTS; do
      I=$(redis-cli -p $p SENTINEL master mymaster 2>/dev/null | tr -d '\r' | paste - - )
      ns=$(echo "$I" | awk -F'\t' '$1=="num-other-sentinels"{print $2}')
      nr=$(echo "$I" | awk -F'\t' '$1=="num-slaves"{print $2}')
      [ "$ns" = 2 ] && [ "$nr" = 2 ] && ok=$((ok+1))
    done
    [ $ok = 3 ] && break; sleep 0.5
  done
  echo "sentinels ready ($ok/3)"
}

P2=""; P3=""
case $S in sen_prio0) P2="replica-priority 0" ;; sen_prio0_all) P2="replica-priority 0"; P3="replica-priority 0" ;; esac
for h in $FO1 $FO2 $FO3; do n "$h" wipeall >/dev/null 2>&1; done
n "$FO1" start "$E" 6379 "appendfsync everysec" >/dev/null
n "$FO2" start "$E" 6379 "replicaof $FO1 6379;$P2" >/dev/null
n "$FO3" start "$E" 6379 "replicaof $FO1 6379;$P3" >/dev/null
for _ in $(seq 1 100); do [ "$(info $FO2:6379 replication master_link_status)$(info $FO3:6379 replication master_link_status)" = upup ] && break; sleep 0.2; done
for h in $FO2 $FO3; do echo "$h slave_priority=$(info $h:6379 replication slave_priority) CONFIG=$(rc $h:6379 CONFIG GET replica-priority | tail -1)"; done | tee "$OUT/priorities.txt"
sentinels_start > "$OUT/sentinels-ready.txt"; cat "$OUT/sentinels-ready.txt"
redis-cli -p 26379 SENTINEL replicas mymaster > "$OUT/sentinel-replicas-before.txt" 2>&1

DUR=75
$PY "$H/ackcheck.py" --mode sentinel --sentinels "$SENTS" --writers ${WRITERS:-16} --duration $DUR --out "$OUT" --events "$EVF" --settle 3 \
  > "$OUT/ackcheck.out" 2>&1 & CK=$!
# who Sentinel names, every 0.2 s
( while kill -0 $CK 2>/dev/null; do echo "$(date +%s.%N) $(smaster)"; sleep 0.2; done ) > "$OUT/master-by-sentinel.txt" &
OLD=$FO1:6379
case $S in
sen_kill9|sen_prio0|sen_prio0_all)
  at 20; n "$FO1" kill9 6379; ev "kill -9 master $OLD"
  at 50; n "$FO1" restart 6379 >/dev/null; ev "old master restarted (no replicaof in its config)" ;;
sen_manual)
  at 20; ev "SENTINEL FAILOVER: $(redis-cli -p 26379 SENTINEL FAILOVER mymaster)" ;;
esac
wait $CK
NEW=$(smaster); echo "master by Sentinel after: $NEW" | tee "$OUT/new-master.txt"
# promoted / role of every node
for h in $FO1 $FO2 $FO3; do echo "$h role=$(info $h:6379 replication role) master_host=$(info $h:6379 replication master_host) link=$(info $h:6379 replication master_link_status)"; done | tee "$OUT/roles-after.txt"
grep -h -E '\+switch-master|\+selected-slave|failover-abort|\+promoted-slave|\+slave-reconf|\+convert-to-slave|\+failover-end|-failover-abort-no-good-slave|\+sdown|\+odown' /data/s26379/sentinel.log > "$OUT/sentinel-events.txt"
cp /data/s26379/sentinel.log "$OUT/sentinel-26379.log"
# convergence: every replica of NEW against NEW
for h in $FO1 $FO2 $FO3; do
  [ "$h:6379" = "$NEW" ] && continue
  for _ in $(seq 1 120); do
    a=$(info "$NEW" replication master_repl_offset); b=$(info "$h:6379" replication slave_repl_offset)
    [ -n "$a" ] && [ "$a" = "$b" ] && break; sleep 0.5
  done
  $PY "$H/kvdiff.py" "$NEW" "$h:6379" --out "$OUT/kvdiff-$h.json" > /dev/null 2>&1
done
for h in $FO1 $FO2 $FO3; do ssh -o LogLevel=ERROR $h 'tail -150 /data/6379/server.log' > "$OUT/server-$h.log" 2>/dev/null; done
sentinels_stop
echo "$S $E rep$REP: $(tail -1 "$OUT/ackcheck.out") new=$NEW"
