#!/usr/bin/env bash
# Tests of scripts/redis_compat_check.sh, the gate on Redis's suite: unit
# logs in the format the suite writes, and what the check makes of each.
#
#   bash scripts/redis_compat_check_test.sh

set -u
CHECK=$(cd "$(dirname "$0")" && pwd)/redis_compat_check.sh
DIR=$(mktemp -d)
trap 'rm -rf "$DIR"' EXIT

cat > "$DIR/known" <<'EOF'
# Known to fail.
unit/alpha | Listed test
unit/alpha | Listed test: now passing
unit/alpha | Listed test in two modes
unit/alpha | Listed test on io_uring | io_uring
EOF

log() { # name, lines of the log after its header
  { echo "Testing $UNIT"; cat; echo; echo "                   The End"; } > "$DIR/$1"
}

failures=0
# want <exit status> <text the output contains, or "" for no output> <log>
want() {
  local out status
  out=$(bash "$CHECK" "$DIR/known" "$UNIT" "$DIR/$3" 2>&1)
  status=$?
  if [ "$status" != "$1" ] || { [ -z "$2" ] && [ -n "$out" ]; } ||
    { [ -n "$2" ] && ! grep -qF -- "$2" <<<"$out"; }; then
    echo "FAIL: $UNIT $3: exit $status (want $1), wanted \"$2\" in:"
    echo "$out"
    failures=$((failures + 1))
  fi
}

UNIT=unit/alpha

log clean <<'EOF'
[ok]: Some test (1 ms)
[ignore]: Another test: Tag: needs:debug denied
EOF
want 0 "" clean

log listed-failure <<'EOF'
[ok]: Some test (1 ms)
[err]: Listed test in tests/unit/alpha.tcl
Expected 1 to equal 2
*** [err]: Listed test in tests/unit/alpha.tcl
EOF
want 0 "" listed-failure

log unlisted-failure <<'EOF'
[ok]: Some test (1 ms)
[err]: Unlisted test in tests/unit/alpha.tcl
Expected 1 to equal 2
EOF
want 1 "Unlisted test" unlisted-failure

log timeout <<'EOF'
[ok]: Some test (1 ms)
[TIMEOUT]: clients state report follows.
sock5b455f9ec430 => (IN PROGRESS) The test that hung
*** [TIMEOUT]: clients state report follows.
EOF
want 1 "The test that hung" timeout

log in-progress <<'EOF'
[ok]: Some test (1 ms)
sock5b455f9ec430 => (IN PROGRESS) The test that hung
EOF
want 1 "The test that hung" in-progress

# Killed by redis_compat.sh's limit on the whole unit: the log just stops.
log unit-timed-out <<'EOF'
[ok]: Some test (1 ms)
[unit timed out]
EOF
want 1 "[unit timed out]" unit-timed-out

log exception <<'EOF'
[ok]: Some test (1 ms)
[exception]: Executing test client: I/O error reading reply.
EOF
want 1 "[exception]" exception

log server-died <<'EOF'
[ok]: Some test (1 ms)
[server died]
EOF
want 1 "[server died]" server-died

echo "[server did not start]" > "$DIR/no-server"
want 1 "[server did not start]" no-server

log listed-passes <<'EOF'
[ok]: Listed test: now passing (12 ms)
EOF
want 0 '::notice::unit/alpha: "Listed test: now passing" passes now' listed-passes

# Fails on io_uring only: passing on Tokio says nothing.
log listed-passes-io-uring <<'EOF'
[ok]: Listed test on io_uring (12 ms)
EOF
want 0 '"Listed test on io_uring" passes now; it is listed as failing on io_uring only' listed-passes-io-uring

# Run once per mode (EVAL and FCALL, RESP2 and RESP3): passing in one of them
# is not passing.
log listed-in-two-modes <<'EOF'
[err]: Listed test in two modes in tests/unit/alpha.tcl
[ok]: Listed test in two modes (3 ms)
EOF
want 0 "" listed-in-two-modes

# A test is listed for its unit only.
UNIT=unit/beta
log other-unit <<'EOF'
[err]: Listed test in tests/unit/beta.tcl
EOF
want 1 "Listed test" other-unit

if [ "$failures" -gt 0 ]; then
  echo "$failures failed"
  exit 1
fi
echo "redis_compat_check: all passed"
