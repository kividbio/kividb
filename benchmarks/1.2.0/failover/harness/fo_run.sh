#!/usr/bin/env bash
# fo_run.sh <engine> <scenario> <rep>
# Runs one failover / durability scenario from the client VM. Server VMs are reached over ssh and driven
# with node.sh. Results: results/<engine>/<scenario>/<rep>/ (summary.json, timeline.csv, events, repl.csv, logs).
#
# Hosts: FO1 FO2 FO3 (server VMs, private IPs) in env or hosts.env next to this script.
set -u
H=$(cd "$(dirname "$0")" && pwd)
[ -f "$H/hosts.env" ] && . "$H/hosts.env"
E=$1 S=$2 REP=${3:-1}
RES=${RES:-$HOME/fo-results}
OUT=$RES/$E/$S/$REP
rm -rf "$OUT"; mkdir -p "$OUT"
PY=$HOME/venv/bin/python
TF=$OUT/target
EVF=$OUT/events
: > "$EVF"
WRITERS=${WRITERS:-16}
n()  { local h=$1; shift; ssh -o StrictHostKeyChecking=no -o LogLevel=ERROR "$h" "bash node.sh $(printf '%q ' "$@")"; }
sx() { local h=$1; shift; ssh -o StrictHostKeyChecking=no -o LogLevel=ERROR "$h" "$@"; }
rc() { local hp=$1; shift; redis-cli -h "${hp%:*}" -p "${hp#*:}" "$@" 2>&1; }
ev() { echo "$(date +%s.%N) $*" >> "$EVF"; echo "  [$(date +%T)] $*"; }
info() { rc "$1" INFO "$2" | tr -d '\r' | awk -F: -v k="$3" '$1==k{print $2; exit}'; }
at() { # at <seconds after the writers started>
  local t0; t0=$(cat "$OUT/t0" 2>/dev/null || true)
  while [ -z "$t0" ]; do sleep 0.05; t0=$(cat "$OUT/t0" 2>/dev/null || true); done
  local now; now=$(date +%s.%N)
  local d; d=$(awk -v a="$t0" -v b="$1" -v c="$now" 'BEGIN{x=a+b-c; printf "%.3f", (x>0?x:0)}')
  sleep "$d"
}
iso()  { sx "$1" "sudo iptables -A INPUT -s $2 -j DROP; sudo iptables -A OUTPUT -d $2 -j DROP"; }
heal() { sx "$1" "sudo iptables -D INPUT -s $2 -j DROP; sudo iptables -D OUTPUT -d $2 -j DROP"; }

# replication poller: every 0.5 s, the role and offsets of the given nodes
repl_poll() { # repl_poll host:port...
  echo "ts,node,role,master_repl_offset,slave_repl_offset,link" > "$OUT/repl.csv"
  while :; do
    for hp in "$@"; do
      I=$(timeout 1 redis-cli -h "${hp%:*}" -p "${hp#*:}" INFO replication 2>/dev/null | tr -d '\r')
      g() { echo "$I" | awk -F: -v k="$1" '$1==k{print $2; exit}'; }
      echo "$(date +%s.%N),$hp,$(g role),$(g master_repl_offset),$(g slave_repl_offset),$(g master_link_status)" >> "$OUT/repl.csv"
    done
    sleep 0.5
  done
}

# ------------------------------------------------------------------ topologies
pair() { # pair "<master directives>" "<replica directives>"
  for h in $FO1 $FO2 $FO3; do n "$h" wipeall >/dev/null 2>&1; done
  n "$FO1" start "$E" 6379 "${1:-}" >/dev/null
  n "$FO2" start "$E" 6379 "replicaof $FO1 6379;${2:-}" >/dev/null
  for _ in $(seq 1 100); do [ "$(info "$FO2:6379" replication master_link_status)" = up ] && break; sleep 0.2; done
  echo "$FO1:6379" > "$TF"
  M=$FO1:6379; R=$FO2:6379
  repl_poll "$M" "$R" & POLL=$!
}
cluster() { # cluster "<directives for every node>"
  local cd="cluster-enabled yes;cluster-node-timeout ${NODE_TIMEOUT:-5000};cluster-config-file nodes.conf;${1:-}"
  for h in $FO1 $FO2 $FO3; do n "$h" wipeall >/dev/null 2>&1; done
  for h in $FO1 $FO2 $FO3; do n "$h" start "$E" 7001 "$cd" >/dev/null; n "$h" start "$E" 7002 "$cd" >/dev/null; done
  redis-cli --cluster create $FO1:7001 $FO2:7001 $FO3:7001 $FO1:7002 $FO2:7002 $FO3:7002 \
    --cluster-replicas 1 --cluster-yes > "$OUT/create.log" 2>&1
  for _ in $(seq 1 150); do
    okc=0
    for h in $FO1 $FO2 $FO3; do for p in 7001 7002; do
      [ "$(rc $h:$p CLUSTER INFO | tr -d '\r' | awk -F: '$1=="cluster_state"{print $2}')" = ok ] && okc=$((okc+1))
    done; done
    [ $okc = 6 ] && break; sleep 0.2
  done
  # every replica attached and synced
  for _ in $(seq 1 100); do
    [ "$(rc $FO1:7001 CLUSTER NODES | grep -c 'slave.*connected')" = 3 ] && break; sleep 0.2
  done
  sleep 2
  rc $FO1:7001 CLUSTER NODES > "$OUT/nodes-before.txt"
  # M: the master owning slot 0; R: its replica
  local mid
  mid=$(awk '$3 ~ /master/ && ($9 ~ /^0-/ || $9 == "0") {print $1}' "$OUT/nodes-before.txt")
  M=$(awk -v id="$mid" '$1==id{split($2,a,"@"); print a[1]}' "$OUT/nodes-before.txt")
  R=$(awk -v id="$mid" '$4==id{split($2,a,"@"); print a[1]}' "$OUT/nodes-before.txt" | head -1)
  SEEDS="$FO1:7001,$FO2:7001,$FO3:7001,$FO1:7002,$FO2:7002,$FO3:7002"
  echo "M=$M R=$R" > "$OUT/roles"
  repl_poll "$M" "$R" & POLL=$!
}

checker() { # checker <duration> [extra ackcheck args]
  local d=$1; shift
  if [ -n "${SEEDS:-}" ]; then
    $PY "$H/ackcheck.py" --mode cluster --seeds "$SEEDS" --writers "$WRITERS" --duration "$d" --out "$OUT" --events "$EVF" "$@" \
      > "$OUT/ackcheck.out" 2>&1 &
  else
    $PY "$H/ackcheck.py" --mode standalone --target-file "$TF" --writers "$WRITERS" --duration "$d" --out "$OUT" --events "$EVF" "$@" \
      > "$OUT/ackcheck.out" 2>&1 &
  fi
  CK=$!
}

finish() {
  wait $CK
  kill $POLL 2>/dev/null
  # convergence of the pair after the run (standalone)
  if [ -z "${SEEDS:-}" ] && [ -n "${CONV_A:-}" ]; then
    for _ in $(seq 1 120); do
      a=$(info "$CONV_A" replication master_repl_offset); b=$(info "$CONV_B" replication slave_repl_offset)
      [ -n "$a" ] && [ "$a" = "$b" ] && break; sleep 0.5
    done
    $PY "$H/../../soak/harness/compare.py" "$CONV_A" "$CONV_B" --out "$OUT/converge.json" > /dev/null 2>&1
  fi
  if [ -n "${SEEDS:-}" ]; then rc $FO1:7001 CLUSTER NODES > "$OUT/nodes-after.txt"; rc $FO1:7001 CLUSTER INFO > "$OUT/cluster-info-after.txt"; fi
  for h in $FO1 $FO2 $FO3; do sx "$h" 'for d in /data/*; do [ -f $d/server.log ] && tail -200 $d/server.log > /tmp/$(basename $d).log; done; tar czf - -C /tmp $(cd /tmp && ls 6379.log 7001.log 7002.log 2>/dev/null)' > "$OUT/logs-$h.tgz" 2>/dev/null; done
  echo "$S $E rep$REP: $(cat "$OUT/ackcheck.out" | tail -1)"
}

echo "== $E $S rep $REP"
case $S in
# ---------------------------------------------------------------- standalone (one master, one replica)
promote_graceful)   # planned switchover: pause writes, wait for the replica, promote it, move clients, demote the old master
  pair; checker 60
  at 20; ev "switchover start (CLIENT PAUSE WRITE on master)"
  rc "$M" CLIENT PAUSE 10000 WRITE >/dev/null
  for _ in $(seq 1 200); do
    a=$(info "$M" replication master_repl_offset); b=$(info "$R" replication slave_repl_offset)
    [ -n "$a" ] && [ "$a" = "$b" ] && break; sleep 0.02
  done
  rc "$R" REPLICAOF NO ONE >/dev/null; echo "$R" > "$TF"
  rc "$M" REPLICAOF "${R%:*}" "${R#*:}" >/dev/null; rc "$M" CLIENT UNPAUSE >/dev/null
  ev "replica promoted, clients moved, old master demoted"
  CONV_A=$R CONV_B=$M; finish ;;
promote_abrupt)     # unplanned: promote the replica while the master is still taking writes
  pair; checker 60
  at 20; rc "$R" REPLICAOF NO ONE >/dev/null; echo "$R" > "$TF"; ev "REPLICAOF NO ONE on replica, clients moved"
  at 22; rc "$M" REPLICAOF "${R%:*}" "${R#*:}" >/dev/null; ev "old master demoted"
  CONV_A=$R CONV_B=$M; finish ;;
kill9_everysec|kill9_always|kill9_noack)
  case $S in kill9_always) X="appendfsync always" ;; kill9_noack) X="aof-ack-after-write no" ;; *) X="appendfsync everysec" ;; esac
  pair "$X"; checker 60
  at 20; n "${M%:*}" kill9 6379; ev "kill -9 master ($X)"
  at 25; n "${M%:*}" restart 6379 >/dev/null; ev "master restarted"
  CONV_A=$M CONV_B=$R; finish ;;
kill9_checkpoint)   # KiviDB: kill -9 the master right as an AOF checkpoint starts (1 KB values make one every few s), 3 times
  pair; checker 90 --payload "$(head -c 1000 /dev/zero | tr '\0' p)"
  for k in 1 2 3; do
    at $((k * 20))
    sx "${M%:*}" "timeout 30 tail -n0 -F /data/6379/server.log | grep -m1 -E 'auto-checkpoint|folding into dump' >/dev/null"
    n "${M%:*}" kill9 6379; ev "kill -9 master as checkpoint $k starts"
    sleep 3; n "${M%:*}" restart 6379 >/dev/null; ev "master restarted"
  done
  CONV_A=$M CONV_B=$R; finish ;;
sigstop)
  pair; checker 80
  at 20; n "${M%:*}" stop 6379; ev "SIGSTOP master"
  at 50; n "${M%:*}" cont 6379; ev "SIGCONT master"
  CONV_A=$M CONV_B=$R; finish ;;
partition)          # master <-> replica cut for 30 s, clients still reach the master
  pair; checker 80
  at 20; iso "${M%:*}" "${R%:*}"; ev "partition master <-> replica"
  at 50; heal "${M%:*}" "${R%:*}"; ev "partition healed"
  CONV_A=$M CONV_B=$R; finish ;;
partition_promote|partition_promote_minrep)  # split brain: replica promoted while the old master still takes writes
  X=""; [ $S = partition_promote_minrep ] && X="min-replicas-to-write 1;min-replicas-max-lag 2"
  pair "$X"; checker 70
  at 20; iso "${M%:*}" "${R%:*}"; ev "partition master <-> replica"
  at 25; rc "$R" REPLICAOF NO ONE >/dev/null; echo "$R" > "$TF"; ev "replica promoted, clients moved"
  at 35; heal "${M%:*}" "${R%:*}"; rc "$M" REPLICAOF "${R%:*}" "${R#*:}" >/dev/null; ev "healed, old master demoted"
  CONV_A=$R CONV_B=$M; finish ;;
wait_kill9_promote) # WAIT 1 after every SET; kill the master, promote the replica
  pair; checker 60 --wait 1
  at 20; n "${M%:*}" kill9 6379; ev "kill -9 master"
  at 21; rc "$R" REPLICAOF NO ONE >/dev/null; echo "$R" > "$TF"; ev "replica promoted, clients moved"
  finish ;;
power_loss_always|power_loss_everysec)  # the master's VM is powered off (no OS shutdown) by pl_drive.sh on the laptop
  X="appendfsync ${S#power_loss_}"
  for h in $FO1 $FO2 $FO3; do n "$h" wipeall >/dev/null 2>&1; done
  n "$FO1" start "$E" 6379 "$X" >/dev/null
  echo "$FO1:6379" > "$TF"; M=$FO1:6379
  repl_poll "$M" & POLL=$!
  checker 330 --timeout 2
  at 30; touch "$OUT/poweroff-now"
  for _ in $(seq 1 600); do [ -f "$OUT/poweroff-at" ] && break; sleep 0.5; done
  echo "$(cat "$OUT/poweroff-at") VM powered off ($X)" >> "$EVF"
  for _ in $(seq 1 1200); do [ -f "$OUT/power-back" ] && break; sleep 0.5; done
  for _ in $(seq 1 120); do sx "$FO1" true 2>/dev/null && break; sleep 1; done
  ev "VM back up"
  n "$FO1" restart 6379 >/dev/null; ev "server restarted"
  sleep 10
  if ! rc "$M" PING | grep -q PONG; then
    # It refused to start: save the evidence, then do what an operator would (cut the torn tail / redis-check-aof --fix)
    sx "$FO1" "tail -3 /data/6379/server.log" > "$OUT/refused-start.txt"
    ev "server refused to start: $(grep -m1 -o -i -E 'bad file format[^—]*|cannot be loaded[^—]*' "$OUT/refused-start.txt" | head -c 120)"
    if [ "$E" = kividb ]; then
      off=$(grep -o -E 'byte offset [0-9]+' "$OUT/refused-start.txt" | head -1 | awk '{print $3}')
      sx "$FO1" "cp /data/6379/appendonly.aof /data/6379/appendonly.aof.torn && truncate -s $off /data/6379/appendonly.aof"
      ev "AOF cut at byte $off (where the zero-filled tail starts); restarted"
    else
      sx "$FO1" "ln -sf /opt/redis/redis-server /opt/redis/redis-check-aof; cd /data/6379/appendonlydir && echo y | /opt/redis/redis-check-aof --fix appendonly.aof.manifest" > "$OUT/redis-check-aof.txt" 2>&1
      ev "redis-check-aof --fix; restarted"
    fi
    n "$FO1" restart 6379 >/dev/null
  fi
  finish ;;
# ---------------------------------------------------------------- cluster (3 masters + 3 replicas)
c_failover|c_failover_force)
  cluster; checker 60
  at 20
  if [ $S = c_failover ]; then rc "$R" CLUSTER FAILOVER >/dev/null; ev "CLUSTER FAILOVER on replica";
  else rc "$R" CLUSTER FAILOVER FORCE >/dev/null; ev "CLUSTER FAILOVER FORCE on replica"; fi
  finish ;;
c_kill9|c_wait_kill9)
  cluster; if [ $S = c_wait_kill9 ]; then checker 70 --wait 1; else checker 70; fi
  at 20; n "${M%:*}" kill9 "${M#*:}"; ev "kill -9 master $M"
  at 40; n "${M%:*}" restart "${M#*:}" >/dev/null; ev "old master restarted"
  finish ;;
c_sigstop)
  cluster; checker 80
  at 20; n "${M%:*}" stop "${M#*:}"; ev "SIGSTOP master $M"
  at 50; n "${M%:*}" cont "${M#*:}"; ev "SIGCONT master"
  finish ;;
c_partition|c_partition_minrep)   # the master's host is cut off from the other servers (not from clients)
  X=""; [ $S = c_partition_minrep ] && X="min-replicas-to-write 1;min-replicas-max-lag 2"
  cluster "$X"; checker 70
  mh=${M%:*}
  others=$(for h in $FO1 $FO2 $FO3; do [ "$h" != "$mh" ] && echo "$h"; done)
  at 20; for o in $others; do iso "$mh" "$o"; done; ev "partition: $mh cut off from the other servers"
  at 40; for o in $others; do heal "$mh" "$o"; done; ev "partition healed"
  finish ;;
*) echo "unknown scenario $S"; exit 2 ;;
esac
