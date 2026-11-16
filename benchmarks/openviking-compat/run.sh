#!/usr/bin/env bash
# Runs OpenViking's Redis-backed tests against KiviDB and against Redis 8, with the same pins:
#
#   1. ragfs (Rust): `cargo test -p ragfs` with REDIS_URL set, which turns on its Redis tests
#      (CacheRuntime, the Redis PathLock provider, script registry), plus the 3-node cluster and
#      password tests in tests/redis_runtime.rs;
#   2. the gateway's suite (tests/gateway), the way OpenViking's gateway.yml workflow runs it:
#      a standalone server and a 3-node cluster, both with the ACL user that workflow creates;
#   3. tests/agfs/test_pathlock_provider_migration.py, filesystem-to-Redis PathLock migration,
#      against the published openviking wheel (it needs the native ragfs binding);
#   4. openviking_expiry_compat.py, for the expiry scheduler, which has no test of its own
#      against a real server.
#
#   ./run.sh               # both servers
#   ./run.sh kividb        # one
#
# Needs Docker, Rust (1.91 or newer), Python 3.10+ and git, on a throwaway Linux machine. It
# starts containers with host networking on ports 6421-6442 and 7100-7202.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REF="${OPENVIKING_REF:-v0.5.3}"
WORK="${WORK:-$HERE/.work}"
SRC="$WORK/openviking"
LABELS=("$@")
[[ ${#LABELS[@]} -gt 0 ]] || LABELS=(kividb redis8)
CLI="docker run --rm --network host redis:8 redis-cli"

mkdir -p "$WORK" "$HERE/logs"
[[ -d "$SRC" ]] || git clone -q --depth 1 --branch "$REF" https://github.com/volcengine/OpenViking.git "$SRC"

# The gateway's own pins (gateway.yml), and the wheel plus the expiry extra for the rest.
[[ -d "$WORK/venv-gateway" ]] || { python3 -m venv "$WORK/venv-gateway"; "$WORK/venv-gateway/bin/pip" install -q -r "$HERE/requirements-gateway.txt"; }
[[ -d "$WORK/venv-openviking" ]] || { python3 -m venv "$WORK/venv-openviking"; "$WORK/venv-openviking/bin/pip" install -q -r "$HERE/requirements.txt"; }

start_servers() { # label -> standalone S, cluster C..C+2, password server P
  local l=$1
  docker rm -f "ov-$l-s" "ov-$l-p" "ov-$l-c0" "ov-$l-c1" "ov-$l-c2" >/dev/null 2>&1 || true
  if [[ $l == kividb ]]; then
    local img=kividb/kividb:v1.2.0-lua
    docker run -d --name "ov-$l-s" --network host $img --port "$S" --protected-mode no >/dev/null
    docker run -d --name "ov-$l-p" --network host $img --port "$P" --requirepass s3cret-pw >/dev/null
    for i in 0 1 2; do
      docker run -d --name "ov-$l-c$i" --network host $img --port $((C + i)) --protected-mode no \
        --cluster-enabled yes --cluster-announce-ip 127.0.0.1 >/dev/null
    done
  else
    docker run -d --name "ov-$l-s" --network host redis:8 redis-server --port "$S" >/dev/null
    docker run -d --name "ov-$l-p" --network host redis:8 redis-server --port "$P" --requirepass s3cret-pw >/dev/null
    for i in 0 1 2; do
      docker run -d --name "ov-$l-c$i" --network host redis:8 redis-server --port $((C + i)) --cluster-enabled yes \
        --cluster-config-file nodes.conf --cluster-node-timeout 5000 --cluster-announce-ip 127.0.0.1 --appendonly no >/dev/null
    done
  fi
  sleep 2
  $CLI --cluster create 127.0.0.1:$C 127.0.0.1:$((C + 1)) 127.0.0.1:$((C + 2)) --cluster-yes >/dev/null
  for _ in $(seq 1 30); do
    $CLI -p "$C" cluster info | grep -q "cluster_state:ok" && break
    sleep 1
  done
  # The gateway user from gateway.yml, on every server.
  for p in "$S" "$C" $((C + 1)) $((C + 2)); do
    $CLI -p "$p" ACL SETUSER gateway reset on '>gateway-test-password' '~ovgw:*' \
      +@read +@write +@scripting +@connection +@transaction +ping +time +scan +unlink '+cluster|slots' '+cluster|shards' >/dev/null
  done
}

stop_servers() {
  docker rm -f "ov-$1-s" "ov-$1-p" "ov-$1-c0" "ov-$1-c1" "ov-$1-c2" >/dev/null 2>&1 || true
}

for l in "${LABELS[@]}"; do
  if [[ $l == kividb ]]; then S=6421; P=6441; C=7100; else S=6422; P=6442; C=7200; fi
  CLUSTER="redis://127.0.0.1:$C,redis://127.0.0.1:$((C + 1)),redis://127.0.0.1:$((C + 2))"
  echo "== $l"
  start_servers "$l"

  echo "   ragfs (cargo test)"
  ( cd "$SRC" && REDIS_URL="redis://127.0.0.1:$S" REDIS_CLUSTER_TEST_URLS="$CLUSTER" \
      REDIS_PASSWORD_TEST_URL="redis://127.0.0.1:$P" REDIS_PASSWORD_TEST_SECRET=s3cret-pw \
      cargo test -p ragfs --features cache,test-utils --no-fail-fast ) > "$HERE/logs/ragfs-$l.txt" 2>&1 || true

  echo "   gateway (pytest tests/gateway)"
  ( cd "$SRC" && OV_TEST_REDIS="redis://127.0.0.1:$S" OV_TEST_REDIS_CLUSTER="$CLUSTER" \
      OV_TEST_REDIS_USERNAME=gateway OV_TEST_REDIS_PASSWORD=gateway-test-password PYTHONPATH=. \
      "$WORK/venv-gateway/bin/pytest" tests/gateway --ignore=tests/gateway/e2e --confcutdir=tests/gateway \
      -o addopts='' -q -p no:cacheprovider --junitxml="$HERE/logs/gateway-$l.xml" ) > "$HERE/logs/gateway-$l.txt" 2>&1 || true

  if [[ $l == kividb ]]; then
    # Again with +command for the gateway user: KiviDB 1.2.0 leaves COMMAND out of @connection
    # (kividbio/kividb#392), and redis-py's cluster client runs COMMAND when it starts.
    echo "   gateway again, the ACL user with +command (#392)"
    for p in "$S" "$C" $((C + 1)) $((C + 2)); do $CLI -p "$p" ACL SETUSER gateway +command >/dev/null; done
    ( cd "$SRC" && OV_TEST_REDIS="redis://127.0.0.1:$S" OV_TEST_REDIS_CLUSTER="$CLUSTER" \
        OV_TEST_REDIS_USERNAME=gateway OV_TEST_REDIS_PASSWORD=gateway-test-password PYTHONPATH=. \
        "$WORK/venv-gateway/bin/pytest" tests/gateway --ignore=tests/gateway/e2e --confcutdir=tests/gateway \
        -o addopts='' -q -p no:cacheprovider --junitxml="$HERE/logs/gateway-$l-plus-command.xml" ) \
        > "$HERE/logs/gateway-$l-plus-command.txt" 2>&1 || true
    for p in "$S" "$C" $((C + 1)) $((C + 2)); do $CLI -p "$p" ACL SETUSER gateway -command >/dev/null; done
  fi

  echo "   pathlock migration (pytest, published wheel)"
  # Run from a copy, so the source tree does not shadow the installed package.
  mkdir -p "$WORK/pathlock" && cp "$SRC/tests/agfs/test_pathlock_provider_migration.py" "$WORK/pathlock/"
  ( cd "$WORK/pathlock" && REDIS_URL="redis://127.0.0.1:$S" "$WORK/venv-openviking/bin/python" -m pytest \
      test_pathlock_provider_migration.py -q -p no:cacheprovider -o asyncio_mode=auto \
      --junitxml="$HERE/logs/pathlock-$l.xml" ) > "$HERE/logs/pathlock-$l.txt" 2>&1 || true

  echo "   expiry scheduler (openviking_expiry_compat.py)"
  ( cd "$WORK" && "$WORK/venv-openviking/bin/python" -I "$HERE/openviking_expiry_compat.py" \
      --url "redis://127.0.0.1:$S" --label "$l" ) > "$HERE/logs/expiry-$l.txt" 2>&1 || true

  version=$($CLI -p "$S" info server | tr -d '\r' | grep -E '^(redis_version|kividb_version):' | tr '\n' ' ')
  python3 "$HERE/summarize.py" "$l" "$version" "$REF"
  stop_servers "$l"
done
