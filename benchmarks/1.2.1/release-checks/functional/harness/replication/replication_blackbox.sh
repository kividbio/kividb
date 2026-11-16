#!/usr/bin/env bash
# Black-box replication verification against the PUBLISHED image for a given
# release tag — three real containers, driven entirely through redis-cli, no
# in-process test binary involved.
#
# This re-asserts, against the compiled release artifact, the same scenarios
# already proven in tests/replication.rs (test_replicaof_repoint_between_masters_converges,
# test_replicaof_same_master_is_noop, test_replicaof_no_one_clears_master_addr)
# — the v1.0.4 replication fix. The bug this is guarding: a replica whose master died could
# never be re-pointed at a new one, because the previous replication task was
# never cancelled and only exited on a role change that never happens when a
# replica is merely moved between masters.
#
# Usage: ./replication_blackbox.sh <image-ref>   (e.g. ghcr.io/kividbio/kividb:v1.0.4-rc1)
set -uo pipefail

IMAGE="${1:?usage: replication_blackbox.sh <image-ref>}"
NET="kivismoke-repl-$$"
PASS=0
FAIL=0

check() { # check <description> <exit-code>
    desc="$1"; got="$2"
    if [ "$got" -eq 0 ]; then
        echo "PASS: $desc"; PASS=$((PASS + 1))
    else
        echo "FAIL: $desc"; FAIL=$((FAIL + 1))
    fi
}

cli() { # cli <host> <redis-cli args...>  -- for status replies (OK/ERR) and INFO dumps.
    host="$1"; shift
    docker run --rm --network "$NET" redis:7-alpine redis-cli -h "$host" -p 6380 --no-raw "$@" 2>&1
}

# Deliberately NOT --no-raw: --no-raw wraps a bulk-string reply (e.g. GET's
# value) in literal double quotes ("1" instead of 1), which would otherwise
# make every value-comparison below compare against the wrong string. INFO's
# multi-line bulk reply is unaffected either way (redis-cli prints it as raw
# lines regardless), which is why `cli` above can stay --no-raw for that use.
cli_val() { # cli_val <host> <redis-cli args...> -> the raw value, unquoted
    host="$1"; shift
    docker run --rm --network "$NET" redis:7-alpine redis-cli -h "$host" -p 6380 "$@" 2>&1
}

info_field() { # info_field <host> <field> -> value or empty, on stdout
    host="$1"; field="$2"
    cli "$host" INFO replication 2>/dev/null | tr -d '\r' | awk -F: -v f="$field" '$1==f{print $2}' | head -1
}

wait_for_field() { # wait_for_field <host> <field> <expected-value> <max-seconds>
    host="$1"; field="$2"; expected="$3"; max="$4"
    i=0
    while [ "$i" -lt "$max" ]; do
        if [ "$(info_field "$host" "$field")" = "$expected" ]; then
            return 0
        fi
        i=$((i + 1))
        sleep 1
    done
    echo "TIMEOUT waiting for $host's $field to become '$expected' (last saw '$(info_field "$host" "$field")')" >&2
    return 1
}

wait_for_get() { # wait_for_get <host> <key> <expected-value> <max-seconds>
    host="$1"; key="$2"; expected="$3"; max="$4"
    i=0
    while [ "$i" -lt "$max" ]; do
        if [ "$(cli_val "$host" GET "$key")" = "$expected" ]; then
            return 0
        fi
        i=$((i + 1))
        sleep 1
    done
    echo "TIMEOUT waiting for $host's key '$key' to become '$expected' (last saw '$(cli_val "$host" GET "$key")')" >&2
    return 1
}

cleanup() {
    echo ""
    echo "=== tearing down ==="
    docker rm -f master-a master-b replica >/dev/null 2>&1
    docker network rm "$NET" >/dev/null 2>&1
}
trap cleanup EXIT

echo "=== testing image: $IMAGE ==="
docker network create "$NET" >/dev/null

echo "=== starting master-a, master-b, replica ==="
docker run -d --name master-a --network "$NET" --network-alias master-a "$IMAGE" --port 6380 --protected-mode no >/dev/null
docker run -d --name master-b --network "$NET" --network-alias master-b "$IMAGE" --port 6380 --protected-mode no >/dev/null
docker run -d --name replica  --network "$NET" --network-alias replica  "$IMAGE" --port 6380 --protected-mode no >/dev/null

wait_for_ping() { # wait_for_ping <host> <max-seconds>
    host="$1"; max="$2"; i=0
    while [ "$i" -lt "$max" ]; do
        cli "$host" PING 2>/dev/null | grep -q PONG && return 0
        i=$((i + 1))
        sleep 1
    done
    echo "TIMEOUT waiting for $host to answer PING" >&2
    return 1
}
wait_for_ping master-a 30
wait_for_ping master-b 30
wait_for_ping replica  30

echo ""
echo "=== baseline: SET on master-a, REPLICAOF replica -> master-a ==="
out=$(cli master-a SET probe 1); echo "$out" | grep -q OK
check "SET probe=1 on master-a" $?

out=$(cli replica REPLICAOF master-a 6380); echo "$out" | grep -q OK
check "REPLICAOF master-a issued on replica" $?

wait_for_field replica master_link_status up 20
check "replica master_link_status:up against master-a" $?
[ "$(info_field replica master_host)" = "master-a" ]
check "replica reports LIVE master_host (not boot config)" $?

wait_for_get replica probe 1 20
check "SET on master-a propagated to replica" $?

echo ""
echo "=== the regression scenario: re-point replica from master-a to master-b, without ever leaving role:slave ==="
out=$(cli replica REPLICAOF master-b 6380); echo "$out" | grep -q OK
check "REPLICAOF master-b issued on replica" $?

wait_for_field replica master_host master-b 20
check "replica INFO now reports master-b (live target, not stale)" $?
[ "$(info_field replica role)" = "slave" ]
check "replica stayed role:slave throughout the re-point" $?

out=$(cli master-b SET probe2 2); echo "$out" | grep -q OK
check "SET probe2=2 on master-b" $?
wait_for_get replica probe2 2 20
check "SET on master-b propagated to replica (new link is live)" $?

echo ""
echo "=== the actual bug: the OLD replication task against master-a must have exited, not kept running ==="
# The old master only notices the closed socket on its next write attempt —
# drive a few writes rather than waiting on a passively-refreshed counter.
i=0
while [ "$i" -lt 15 ]; do
    cli master-a SET "probe_drain_$i" x >/dev/null 2>&1
    [ "$(info_field master-a connected_slaves)" = "0" ] && break
    i=$((i + 1))
    sleep 1
done
[ "$(info_field master-a connected_slaves)" = "0" ]
check "stale task disconnected from master-a (connected_slaves:0)" $?
[ "$(info_field master-b connected_slaves)" = "1" ]
check "master-b sees exactly one replica, not a duplicate registration" $?

echo ""
echo "=== repeated REPLICAOF to the already-current master is a no-op, not a fresh resync ==="
for _ in 1 2 3; do
    out=$(cli replica REPLICAOF master-b 6380); echo "$out" | grep -q OK
    check "repeated REPLICAOF master-b stays OK" $?
done
wait_for_field replica master_link_status up 5
check "link stays up across repeated identical REPLICAOF calls" $?

echo ""
echo "=== REPLICAOF NO ONE promotes and clears the reported master ==="
out=$(cli replica REPLICAOF NO ONE); echo "$out" | grep -q OK
check "REPLICAOF NO ONE issued on replica" $?
wait_for_field replica role master 10
check "role:master after REPLICAOF NO ONE" $?
if [ -z "$(info_field replica master_host)" ]; then master_cleared=0; else master_cleared=1; fi
check "promotion clears the recorded master (master_host absent)" "$master_cleared"

echo ""
echo "=== summary: $PASS passed, $FAIL failed (image: $IMAGE) ==="
[ "$FAIL" -eq 0 ]
