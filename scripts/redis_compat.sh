#!/usr/bin/env bash
# Redis's own test suite, run against a KiviDB binary — one unit at a time.
#
#   scripts/redis_compat.sh <kividb-binary> <redis-checkout> [<output-dir>]
#
# Each unit of the suite gets a server of its own, started empty. The suite
# gives up on its whole run the moment one test stalls, and a unit that
# leaves the server in an odd state (a memory limit set and not reset, a
# client left blocked) takes the units after it down with it: run together,
# one bad test hid most of the suite — eleven units of ninety ran, and the
# number reported was whatever those eleven came to. Run apart, a unit that
# hangs or dies costs itself, and what is reported is the suite.
#
# Prints a line per unit and a total; writes the same as a table to
# <output-dir>/summary.md (and to $GITHUB_STEP_SUMMARY, under Actions), and
# each unit's full log to <output-dir>/<unit>.log.
#
# A unit that fails the gate (scripts/redis_compat_check.sh: a failure not
# in scripts/redis_compat_known_failures.txt, or an early end) is run once
# more, its first log kept as <unit>.1.log: a regression fails twice, a
# flake once.
#
# Environment:
#   UNITS         the units to run (default: unit/*.tcl and unit/type/*.tcl)
#   PORT          the port to run the server on (default 6399)
#   TEST_TIMEOUT  seconds without a test finishing before a unit is given up
#                 on (default 300)
#   UNIT_TIMEOUT  seconds a unit may take in all (default 900)
#   SERVER_ARGS   more arguments for the server
#
# Exits 1 if a unit fails the gate both times, or the port is already in use.

set -u

HERE=$(dirname "$(readlink -f "$0")")
CHECK=$HERE/redis_compat_check.sh
KNOWN=$HERE/redis_compat_known_failures.txt
BIN=$(readlink -f "${1:?usage: redis_compat.sh <kividb-binary> <redis-checkout> [<output-dir>]}")
SUITE=$(readlink -f "${2:?usage: redis_compat.sh <kividb-binary> <redis-checkout> [<output-dir>]}")
OUT=${3:-/tmp/redis-compat}
PORT=${PORT:-6399}
TEST_TIMEOUT=${TEST_TIMEOUT:-300}
UNIT_TIMEOUT=${UNIT_TIMEOUT:-900}

mkdir -p "$OUT"
OUT=$(readlink -f "$OUT")

# Tests that need what a server reached from outside cannot give them: a
# server the suite started itself (`external:skip`), Redis's DEBUG command,
# a replica, an RDB file on disk, a cluster. Everything else is run.
TAGS=(
  --tags -external:skip
  --tags -needs:debug
  --tags -needs:repl
  --tags -needs:save
  --tags -needs:pfdebug
  --tags -large-memory
  --tags -cluster:skip
  --tags -keysizes
)

cd "$SUITE" || exit 1
if [ -z "${UNITS:-}" ]; then
  UNITS=$(cd tests && ls unit/*.tcl unit/type/*.tcl | sed 's/\.tcl$//')
fi

listening() { (exec 3<>"/dev/tcp/127.0.0.1/$PORT") 2>/dev/null; }

# The server shares its port with any other process that asks for it
# (SO_REUSEPORT): one left over from another run would take some of the
# suite's connections, and the results would mean nothing.
if listening; then
  echo "something is already listening on port $PORT; stop it, or set PORT" >&2
  exit 1
fi

server_pid=
start_server() {
  local dir
  dir=$(mktemp -d)
  (cd "$dir" && exec "$BIN" --port "$PORT" --loglevel error ${SERVER_ARGS:-}) > "$OUT/server.log" 2>&1 &
  server_pid=$!
  for _ in $(seq 1 100); do
    listening && return 0
    sleep 0.1
  done
  return 1
}
stop_server() {
  [ -n "$server_pid" ] && kill -9 "$server_pid" 2>/dev/null
  wait "$server_pid" 2>/dev/null
  server_pid=
}
trap stop_server EXIT

count() { grep -ac "$1" "$2" 2>/dev/null || true; }

{
  echo "| unit | passed | failed | aborted | seconds |"
  echo "|---|---:|---:|---:|---:|"
} > "$OUT/summary.md"

# One run of unit $1 against a server of its own, logged to $2.
run_unit() {
  if ! start_server; then
    echo "[server did not start]" > "$2"
    stop_server
    return
  fi
  timeout "$UNIT_TIMEOUT" ./runtest \
    --host 127.0.0.1 --port "$PORT" --singledb --ignore-encoding \
    --timeout "$TEST_TIMEOUT" --single "$1" "${TAGS[@]}" > "$2" 2>&1
  # Killed by `timeout`, the suite says nothing: its log just stops.
  [ $? -ne 124 ] || echo "[unit timed out]" >> "$2"
  kill -0 "$server_pid" 2>/dev/null || echo "[server died]" >> "$2"
  stop_server
}

total_ok=0 total_err=0 total_aborted=0 failed_units=
for unit in $UNITS; do
  log="$OUT/$(echo "$unit" | tr '/' '_').log"
  started=$(date +%s)
  run_unit "$unit" "$log"
  if ! bash "$CHECK" "$KNOWN" "$unit" "$log"; then
    mv "$log" "${log%.log}.1.log"
    echo "$unit: failed the gate; running it again"
    run_unit "$unit" "$log"
    bash "$CHECK" "$KNOWN" "$unit" "$log" || failed_units="$failed_units $unit"
  fi
  seconds=$(( $(date +%s) - started ))

  ok=$(count '^\[ok\]' "$log")
  err=$(count '^\[err\]' "$log")
  # A unit that ends early — on an exception in the test code, a test that
  # never finished, its time running out, or the server gone — has tests
  # after that point that were not run.
  aborted=$(( $(count '^\[exception\]' "$log") + $(count '^\[TIMEOUT\]' "$log") + $(count '^\[unit timed out\]' "$log") + $(count '^\[server ' "$log") ))

  total_ok=$((total_ok + ok))
  total_err=$((total_err + err))
  total_aborted=$((total_aborted + aborted))
  printf '%-34s passed=%-4s failed=%-3s aborted=%-2s %ss\n' "$unit" "$ok" "$err" "$aborted" "$seconds"
  # Units with nothing in them for an external server are left out of the
  # table: most of the suite's ninety files are replication and cluster.
  if [ $((ok + err + aborted)) -gt 0 ]; then
    echo "| $unit | $ok | $err | $aborted | $seconds |" >> "$OUT/summary.md"
  fi
done

total="Redis test suite: $total_ok passed, $total_err failed, $total_aborted units ended early"
verdict="Failed the gate twice:$failed_units"
[ -n "$failed_units" ] || verdict="No failures outside scripts/redis_compat_known_failures.txt"
echo
echo "$total"
echo "$verdict"
{
  echo "| **total** | **$total_ok** | **$total_err** | **$total_aborted** | |"
  echo
  echo "$total."
  echo
  echo "$verdict."
  if [ "$total_err" -gt 0 ]; then
    echo
    echo "<details><summary>Failed tests</summary>"
    echo
    echo '```'
    grep -ah '^\[err\]' "$OUT"/*.log | cut -c1-200
    echo '```'
    echo "</details>"
  fi
} >> "$OUT/summary.md"
if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
  { echo "## Redis compatibility"; echo; cat "$OUT/summary.md"; } >> "$GITHUB_STEP_SUMMARY"
fi
[ -z "$failed_units" ] || exit 1
