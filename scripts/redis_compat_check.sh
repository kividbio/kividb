#!/usr/bin/env bash
# Whether one unit's log from Redis's test suite passes the gate:
#
#   scripts/redis_compat_check.sh <known-failures> <unit> <log>
#
# Exits 1, printing why, if a test failed that is not listed for this unit
# in <known-failures>, or if the unit ended early: an exception, a test that
# never finished, the server dying or not starting. The tests after that
# point did not run, so an early end is never a pass.
#
# A listed test that passed and failed nowhere in the log is printed as a
# GitHub notice, so it is taken off the list; it does not fail the gate.
# The test is checked as a whole because some units run a test once per
# mode (EVAL and FCALL, RESP2 and RESP3), under one name.
#
# Tests: scripts/redis_compat_check_test.sh.

set -u
KNOWN=$1 UNIT=$2 LOG=$3

# "<unit> | <test name>" lines; the names listed for this unit.
known=$(grep -v '^#' "$KNOWN" | awk -F' [|] ' -v unit="$UNIT" '$1 == unit { print $2 }')

# "[err]: <test name> in tests/<unit>.tcl". A line that names another file
# keeps it, and so matches nothing listed.
failed=$(awk -v suffix=" in tests/$UNIT.tcl" '
  index($0, "[err]: ") == 1 {
    name = substr($0, 8)
    end = length(name) - length(suffix)
    if (end > 0 && substr(name, end + 1) == suffix) name = substr(name, 1, end)
    print name
  }' "$LOG")

status=0
while IFS= read -r name; do
  [ -n "$name" ] || continue
  if ! grep -qxF -- "$name" <<<"$known"; then
    echo "$UNIT: failed: $name"
    status=1
  fi
done <<<"$failed"

while IFS= read -r line; do
  [ -n "$line" ] || continue
  echo "$UNIT: ended early: $line"
  status=1
done < <(grep -aE '^\[(exception|TIMEOUT)\]|^\[server (died|did not start)\]' "$LOG")
while IFS= read -r name; do
  [ -n "$name" ] && echo "$UNIT: did not finish: $name"
done < <(grep -ao '(IN PROGRESS) .*' "$LOG" | cut -c15-)

while IFS= read -r name; do
  [ -n "$name" ] || continue
  grep -qxF -- "$name" <<<"$failed" && continue
  if awk -v ok="[ok]: $name (" 'index($0, ok) == 1 { found = 1 } END { exit !found }' "$LOG"; then
    echo "::notice::$UNIT: \"$name\" passes now; remove it from ${KNOWN##*/}"
  fi
done <<<"$known"

exit $status
