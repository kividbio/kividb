#!/usr/bin/env bash
# run.sh: Redis Sentinel managing KiviDB. Three KiviDB nodes (a master and
# two replicas, each from a config file, with a password) and three Redis
# Sentinels watching them, quorum 2, with a password of their own. Each round
# runs one client against the Sentinels and fails the master over one way:
#
#   kill     the master is killed (SIGKILL), and started again after;
#   manual   SENTINEL FAILOVER, the master up throughout;
#   pause    the master is paused (SIGSTOP, or docker pause) until a replica
#            has been promoted, then let go: it comes back as a master, and
#            Sentinel turns it into a replica.
#
# A kill round runs for each client; the manual and pause rounds run with
# the first. In every round:
#
#   1. the client connects through the Sentinels and writes to the master;
#   2. the Sentinels promote a replica, and the other replica follows it;
#   3. the client goes on writing, to the new master, with no restart;
#   4. the old master comes back as a replica of the new one, with the
#      client's writes;
#   5. the config files say so (Sentinel follows each REPLICAOF with CONFIG
#      REWRITE): no `replicaof` in the new master's, the new master in the
#      others'.
#
# The clients are redis-py, ioredis and Lettuce (follow_*.*). Each is run
# with SENTINELS, SENTINEL_PASSWORD, MASTER, PASSWORD, KEY and DEADLINE in
# its environment, prints `ready <port>` once it has written to the master,
# and exits 0 once it has written ten times more to a master on another port
# and read the last write back, or 1 at the deadline.
#
#   KIVIDB_BIN | KIVIDB_IMAGE    the server: a binary, or an image run with
#                                host networking (Linux)
#   SENTINEL_BIN | REDIS_IMAGE   Redis Sentinel: a redis-sentinel binary, or
#                                a Redis image (default redis:8.10.3, by
#                                digest)
#   NODE_KIND                    kividb (default), or redis to run the same
#                                checks on redis-server nodes (KIVIDB_BIN is
#                                then a redis-server binary, or REDIS_IMAGE
#                                is used), to tell a test problem from a
#                                KiviDB one
#   CLIENTS                      default "redis-py ioredis lettuce"
#   WORK                         where the nodes' files go (default a temp dir)
#
# The clients are set up first, each at a pinned version: a venv in WORK
# for redis-py (requirements.txt), npm ci for ioredis (package-lock.json),
# mvn package for Lettuce (lettuce/pom.xml).
# A summary goes to $GITHUB_STEP_SUMMARY when set. Exits 1 on the first
# check that fails, with the logs of every node and Sentinel.
set -euo pipefail

HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
NODE_KIND=${NODE_KIND:-kividb}
REDIS_IMAGE=${REDIS_IMAGE:-redis:8.10.3@sha256:1f88291b4c57bdf249f02eb085a8669889b2d2954e91d4de5118a5da6b78dd53}
CLIENTS=${CLIENTS:-redis-py ioredis lettuce}
WORK=${WORK:-$(mktemp -d)}
mkdir -p "$WORK"
WORK=$(cd "$WORK" && pwd)
PASSWORD=sentinel-test-password
SENTINEL_PASSWORD=sentinel-test-sentinels-password
MASTER=m
NODES=(17101 17102 17103)
SENTINEL_PORTS=(27101 27102 27103)
SENTINELS=$(printf '127.0.0.1:%s,' "${SENTINEL_PORTS[@]}")
SENTINELS=${SENTINELS%,}
CONTAINERS=()
PIDS=() # pids of the processes started, by binary

if [ -z "${KIVIDB_BIN:-}" ] && [ -z "${KIVIDB_IMAGE:-}" ] && [ "$NODE_KIND" = kividb ]; then
  echo "set KIVIDB_BIN or KIVIDB_IMAGE" >&2
  exit 2
fi

say() { echo "== $*"; }
summary() { if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then echo "$*" >>"$GITHUB_STEP_SUMMARY"; fi; }
cli() { python3 "$HERE/cli.py" "$@"; }

cleanup() {
  for c in ${CONTAINERS[@]+"${CONTAINERS[@]}"}; do docker rm -f "$c" >/dev/null 2>&1 || true; done
  for p in ${PIDS[@]+"${PIDS[@]}"}; do kill -9 "$p" 2>/dev/null || true; done
}
# The pid of the node on port $1, when it was started from a binary.
pid_of() { eval "echo \${PID_$1:-}"; }
trap cleanup EXIT

fail() {
  echo "FAIL: $*" >&2
  summary "❌ $*"
  for c in ${CONTAINERS[@]+"${CONTAINERS[@]}"}; do
    echo "---- container $c (last 40 lines)" >&2
    docker logs --tail 40 "$c" >&2 2>&1 || true
  done
  for f in "$WORK"/*/log; do
    echo "---- $f (last 40 lines)" >&2
    tail -n 40 "$f" >&2 || true
  done
  exit 1
}

# wait_for SECONDS DESCRIPTION COMMAND...: runs COMMAND every half second
# until it succeeds.
wait_for() {
  local secs=$1 what=$2
  shift 2
  local end=$((SECONDS + secs))
  until "$@" >/dev/null 2>&1; do
    [ $SECONDS -lt "$end" ] || fail "$what: not within ${secs}s"
    sleep 0.5
  done
}

# ── Nodes and Sentinels ──────────────────────────────────────────────────

# In a container the node's directory is mounted at /data.
node_dir_seen() { if [ -n "${KIVIDB_IMAGE:-}" ] || { [ "$NODE_KIND" = redis ] && [ -z "${KIVIDB_BIN:-}" ]; }; then echo /data; else echo "$WORK/$1"; fi; }

write_node_conf() {
  local port=$1 master=${2:-}
  mkdir -p "$WORK/$port"
  chmod 777 "$WORK/$port" # the image's user writes here, and CONFIG REWRITE the file
  {
    echo "port $port"
    echo "bind 127.0.0.1"
    echo "dir $(node_dir_seen "$port")"
    echo "requirepass $PASSWORD"
    echo "masterauth $PASSWORD"
    [ -n "$master" ] && echo "replicaof 127.0.0.1 $master"
  } >"$WORK/$port/node.conf"
  chmod 666 "$WORK/$port/node.conf"
}

start_node() {
  local port=$1 conf
  conf="$(node_dir_seen "$port")/node.conf"
  if [ "$NODE_KIND" = kividb ] && [ -n "${KIVIDB_BIN:-}" ]; then
    "$KIVIDB_BIN" --configfile "$conf" >>"$WORK/$port/log" 2>&1 &
    eval "PID_$port=$!"
    PIDS+=($!)
  elif [ "$NODE_KIND" = redis ] && [ -n "${KIVIDB_BIN:-}" ]; then
    "$KIVIDB_BIN" "$conf" >>"$WORK/$port/log" 2>&1 &
    eval "PID_$port=$!"
    PIDS+=($!)
  else
    local name=sentinel-test-node-$port image=${KIVIDB_IMAGE:-} args=(--configfile "$conf")
    if [ "$NODE_KIND" = redis ]; then image=$REDIS_IMAGE args=(redis-server "$conf"); fi
    docker rm -f "$name" >/dev/null 2>&1 || true
    docker run -d --name "$name" --network host --user "$(id -u):$(id -g)" \
      -v "$WORK/$port:/data" "$image" "${args[@]}" >/dev/null
    CONTAINERS+=("$name")
  fi
}

# pause_node PORT / resume_node PORT: SIGSTOP and SIGCONT, or docker pause and unpause.
pause_node() {
  local pid
  pid=$(pid_of "$1")
  if [ -n "$pid" ]; then kill -STOP "$pid"; else docker pause "sentinel-test-node-$1" >/dev/null; fi
}
resume_node() {
  local pid
  pid=$(pid_of "$1")
  if [ -n "$pid" ]; then kill -CONT "$pid"; else docker unpause "sentinel-test-node-$1" >/dev/null; fi
}

kill_node() {
  local port=$1
  local pid
  pid=$(pid_of "$port")
  if [ -n "$pid" ]; then
    kill -9 "$pid"
    wait "$pid" 2>/dev/null || true
    eval "PID_$port="
  else
    docker logs "sentinel-test-node-$port" >>"$WORK/$port/log" 2>&1 || true
    docker rm -f "sentinel-test-node-$port" >/dev/null
  fi
}

start_sentinel() {
  local port=$1 dir=$WORK/sentinel-$1
  mkdir -p "$dir"
  chmod 777 "$dir" # Sentinel rewrites its config file
  cat >"$dir/sentinel.conf" <<EOF
port $port
bind 127.0.0.1
sentinel resolve-hostnames no
requirepass $SENTINEL_PASSWORD
sentinel sentinel-pass $SENTINEL_PASSWORD
sentinel monitor $MASTER 127.0.0.1 ${NODES[0]} 2
sentinel auth-pass $MASTER $PASSWORD
sentinel down-after-milliseconds $MASTER 2000
sentinel failover-timeout $MASTER 10000
EOF
  chmod 666 "$dir/sentinel.conf"
  if [ -n "${SENTINEL_BIN:-}" ]; then
    "$SENTINEL_BIN" "$dir/sentinel.conf" >>"$dir/log" 2>&1 &
    PIDS+=($!)
  else
    local name=sentinel-test-sentinel-$port
    docker rm -f "$name" >/dev/null 2>&1 || true
    docker run -d --name "$name" --network host --user "$(id -u):$(id -g)" \
      -v "$dir:/data" "$REDIS_IMAGE" redis-sentinel /data/sentinel.conf >/dev/null
    CONTAINERS+=("$name")
  fi
}

moved_from() { [ "$(master_port)" != "$1" ]; }
# The node on port $1 reads $3 from key $2.
holds() { [ "$(cli "$1" "$PASSWORD" GET "$2")" = "$3" ]; }
master_port() { cli "${SENTINEL_PORTS[0]}" "$SENTINEL_PASSWORD" SENTINEL GET-MASTER-ADDR-BY-NAME "$MASTER" | tail -n 1; }
# What every Sentinel names as the master is $1.
all_name() { for s in "${SENTINEL_PORTS[@]}"; do [ "$(cli "$s" "$SENTINEL_PASSWORD" SENTINEL GET-MASTER-ADDR-BY-NAME "$MASTER" | tail -n 1)" = "$1" ] || return 1; done; }
role_of() { cli "$1" "$PASSWORD" ROLE | head -n 1; }
# $1 is a replica of $2, with its link up.
replica_of() {
  local info
  info=$(cli "$1" "$PASSWORD" INFO replication) || return 1
  grep -q "^role:slave" <<<"$info" && grep -q "^master_port:$2" <<<"$info" && grep -q "^master_link_status:up" <<<"$info"
}
# Every Sentinel knows the other two, and both replicas of the master.
sentinels_see() {
  for s in "${SENTINEL_PORTS[@]}"; do
    local m
    m=$(cli "$s" "$SENTINEL_PASSWORD" SENTINEL MASTER "$MASTER" | paste - -) || return 1
    grep -q "^num-slaves	2$" <<<"$m" && grep -q "^num-other-sentinels	2$" <<<"$m" || return 1
  done
}

# ── Clients ──────────────────────────────────────────────────────────────

setup_clients() {
  for client in $CLIENTS; do
    case $client in
    redis-py)
      python3 -m venv "$WORK/venv"
      "$WORK/venv/bin/pip" install -q -r "$HERE/requirements.txt"
      ;;
    ioredis) npm ci --silent --no-audit --no-fund --prefix "$HERE" ;;
    lettuce) mvn -q -B -f "$HERE/lettuce/pom.xml" package ;;
    *) echo "unknown client $client" >&2 && exit 2 ;;
    esac
  done
}

run_client() {
  local client=$1 out=$2
  export SENTINELS SENTINEL_PASSWORD MASTER PASSWORD KEY="sentinel-test:$1" DEADLINE=60
  case $client in
  redis-py) "$WORK/venv/bin/python" "$HERE/follow_redis_py.py" ;;
  ioredis) node "$HERE/follow_ioredis.js" ;;
  lettuce) java -cp "$HERE/lettuce/target/classes:$HERE/lettuce/target/lib/*" FollowLettuce ;;
  esac >"$out" 2>&1
}

# ── The run ──────────────────────────────────────────────────────────────

say "setting up the clients: $CLIENTS"
setup_clients

say "starting the nodes ($NODE_KIND) and the Sentinels in $WORK"
write_node_conf "${NODES[0]}"
write_node_conf "${NODES[1]}" "${NODES[0]}"
write_node_conf "${NODES[2]}" "${NODES[0]}"
for p in "${NODES[@]}"; do start_node "$p"; done
for p in "${SENTINEL_PORTS[@]}"; do start_sentinel "$p"; done
wait_for 30 "the replicas syncing" replica_of "${NODES[1]}" "${NODES[0]}"
wait_for 30 "the replicas syncing" replica_of "${NODES[2]}" "${NODES[0]}"
wait_for 60 "every Sentinel seeing both replicas and the other Sentinels" sentinels_see
summary "### Redis Sentinel ($REDIS_IMAGE) managing $NODE_KIND ${KIVIDB_IMAGE:-${KIVIDB_BIN:-$REDIS_IMAGE}}"
summary ""
summary "| Round | Client | Old master | Promoted | Client followed | Old master back |"
summary "|---|---|---|---|---|---|"

# round HOW CLIENT: fails the master over HOW (kill, manual or pause), with
# CLIENT writing through the Sentinels, and checks steps 1 to 5 above.
round() {
  local how=$1 client=$2 old new out pid t0 promoted n p
  old=$(master_port)
  say "$how, $client: writing through the Sentinels to the master on $old"
  out=$WORK/$how-$client.out
  run_client "$client" "$out" &
  pid=$!
  wait_for 60 "$client writing to the master" grep -q "^ready $old" "$out"

  t0=$SECONDS
  case $how in
  kill) say "killing the master on $old" && kill_node "$old" ;;
  manual)
    say "SENTINEL FAILOVER"
    cli "${SENTINEL_PORTS[1]}" "$SENTINEL_PASSWORD" SENTINEL FAILOVER "$MASTER" >/dev/null ||
      fail "SENTINEL FAILOVER refused"
    ;;
  pause) say "pausing the master on $old" && pause_node "$old" ;;
  esac
  wait_for 60 "a replica promoted in place of $old" moved_from "$old"
  new=$(master_port)
  wait_for 30 "every Sentinel naming $new" all_name "$new"
  [ "$(role_of "$new")" = master ] || fail "Sentinel names $new, which says it is a $(role_of "$new")"
  promoted=$((SECONDS - t0))
  for p in "${NODES[@]}"; do
    [ "$p" = "$old" ] || [ "$p" = "$new" ] || wait_for 30 "$p following $new" replica_of "$p" "$new"
  done
  say "$new promoted after ${promoted}s"

  wait "$pid" || fail "$client did not follow the failover: $(tail -n 3 "$out")"
  grep -q "^followed $old -> $new" "$out" || fail "$client: $(tail -n 3 "$out")"
  say "$client: $(tail -n 1 "$out")"

  case $how in
  kill) say "starting the old master on $old again" && start_node "$old" ;;
  pause) say "letting the old master on $old go on" && resume_node "$old" ;;
  esac
  wait_for 60 "$old back as a replica of $new" replica_of "$old" "$new"
  n=$(cli "$new" "$PASSWORD" GET "sentinel-test:$client")
  wait_for 30 "$old holding $client's writes" holds "$old" "sentinel-test:$client" "$n"

  # Sentinel follows each REPLICAOF with CONFIG REWRITE: a restart keeps the roles.
  ! grep -qi "^replicaof" "$WORK/$new/node.conf" ||
    fail "the new master $new's config file still names a master: $(grep -i replicaof "$WORK/$new/node.conf")"
  for p in "${NODES[@]}"; do
    [ "$p" = "$new" ] && continue
    wait_for 30 "$p's config file naming $new" grep -qi "^replicaof 127.0.0.1 $new" "$WORK/$p/node.conf"
  done
  wait_for 60 "every Sentinel seeing both replicas of $new" sentinels_see
  summary "| $how | $client | $old | $new after ${promoted}s | $(sed -n 's/^followed .* after \(.*\)/yes, \1/p' "$out") | yes |"
}

for client in $CLIENTS; do round kill "$client"; done
first=${CLIENTS%% *}
round manual "$first"
round pause "$first"

say "passed: kill with $CLIENTS, manual and pause with $first"
summary ""
summary "✅ every failover was followed"
