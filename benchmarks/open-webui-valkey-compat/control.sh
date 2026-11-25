#!/usr/bin/env bash
# control.sh DIR: the same checks as ci.sh, against valkey/valkey-bundle (the server Open WebUI's Valkey
# vector store is written for), which must pass every one. DIR is where `ci.sh setup` put the client
# (benchmarks/ci/run-harness.sh open-webui-valkey runs it first).
#   VALKEY_IMAGE  the control server, pinned by digest (default: valkey-bundle 9.2.0)
#   OUT           where the results go (default ./compat-results)
#   PORT          the server's port (default 6379)
# Exits 1 when a check fails on the control, or it did not run: then the checks, not KiviDB, are wrong.
set -uo pipefail
here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
dir=$(cd "$1" && pwd)
VALKEY_IMAGE=${VALKEY_IMAGE:-valkey/valkey-bundle:9.2.0@sha256:cc210fe648eea982a9b3d76c429859ca150e2b653aa9b3cd81e58c9b49940404}
OUT=$(mkdir -p "${OUT:-compat-results}" && cd "${OUT:-compat-results}" && pwd)
PORT=${PORT:-6379}
name=compat-open-webui-valkey-control-$$
summary() { if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then tee -a "$GITHUB_STEP_SUMMARY"; else cat; fi; }

docker run -d --name "$name" --network host "$VALKEY_IMAGE" --port "$PORT" --save "" --appendonly no --protected-mode no >/dev/null || exit 1
up=
for _ in $(seq 1 30); do
  if exec 3<>"/dev/tcp/127.0.0.1/$PORT" 2>/dev/null; then
    printf 'PING\r\n' >&3
    if read -r -t 2 reply <&3 && [[ $reply == +PONG* ]]; then up=1; fi
    exec 3>&-
  fi
  [ -n "$up" ] && break
  sleep 1
done
if [ -z "$up" ]; then
  docker logs "$name" >"$OUT/open-webui-valkey-control-server.log" 2>&1
  docker stop "$name" >/dev/null 2>&1
  printf '### open-webui-valkey control\n\nThe control server did not start: see the step log.\n\n' | summary
  exit 1
fi
result=$OUT/open-webui-valkey-control.xml
bash "$here/ci.sh" run "$dir" "$PORT" "$result" || echo "open-webui-valkey control: no results"
docker logs "$name" >"$OUT/open-webui-valkey-control-server.log" 2>&1
docker stop "$name" >/dev/null 2>&1

tests=$(grep -o '<testcase ' "$result" 2>/dev/null | wc -l)
failed=$(grep -o '<failure ' "$result" 2>/dev/null | wc -l)
{
  printf '### open-webui-valkey control\n\n`%s`: %s of %s checks pass.\n\n' "$VALKEY_IMAGE" "$((tests - failed))" "$tests"
  [ "$tests" -gt 0 ] && [ "$failed" = 0 ] || printf 'The control must pass every check: see the step log.\n\n'
} | summary
[ "$tests" -gt 0 ] && [ "$failed" = 0 ]
