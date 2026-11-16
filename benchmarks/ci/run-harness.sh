#!/usr/bin/env bash
# run-harness.sh NAME: runs benchmarks/NAME-compat/ci.sh against Redis, then
# against KiviDB, and compares the results check by check (compare.py).
#
#   KIVIDB_IMAGE, REDIS_IMAGE  the servers, both pinned (required)
#   OUT                        where the results go (default ./compat-results)
#   WORK                       where the harness sets itself up (default ./compat-work)
#   PORT                       the server's port (default 6379)
#
# Each server starts fresh, with host networking, on 127.0.0.1:PORT. The
# comparison goes to $GITHUB_STEP_SUMMARY when set, else to stdout. Exits 1
# only when a check passes on Redis and fails on KiviDB. A harness that
# cannot set itself up, or whose run against Redis gives no results, is
# reported as a warning: that is not KiviDB's failure, and it says nothing
# about KiviDB either.
set -uo pipefail
NAME=$1
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
HARNESS=$(cd "$HERE/.." && pwd)/$NAME-compat/ci.sh
: "${KIVIDB_IMAGE:?set KIVIDB_IMAGE}" "${REDIS_IMAGE:?set REDIS_IMAGE}"
OUT=$(mkdir -p "${OUT:-compat-results}" && cd "${OUT:-compat-results}" && pwd)
WORK=$(mkdir -p "${WORK:-compat-work}/$NAME" && cd "${WORK:-compat-work}/$NAME" && pwd)
PORT=${PORT:-6379}
CONTAINER=compat-$NAME

summary() { if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then tee -a "$GITHUB_STEP_SUMMARY"; else cat; fi; }

# start_server IMAGE: a fresh server on 127.0.0.1:PORT, answering PING.
start_server() {
  docker rm -f "$CONTAINER" >/dev/null 2>&1
  case $1 in
  redis:* | */redis:*) docker run -d --name "$CONTAINER" --network host "$1" --port "$PORT" --save "" --appendonly no >/dev/null ;;
  *) docker run -d --name "$CONTAINER" --network host "$1" --port "$PORT" --protected-mode no >/dev/null ;;
  esac || return 1
  for _ in $(seq 1 30); do
    if exec 3<>"/dev/tcp/127.0.0.1/$PORT" 2>/dev/null; then
      printf 'PING\r\n' >&3
      if read -r -t 2 reply <&3 && [[ $reply == +PONG* ]]; then
        exec 3>&-
        return 0
      fi
      exec 3>&-
    fi
    sleep 1
  done
  return 1
}

echo "::group::$NAME: setup"
if ! bash "$HARNESS" setup "$WORK"; then
  echo "::endgroup::"
  echo "::warning title=$NAME::setup failed, not compared"
  printf '### %s\n\nSetup failed, so it was not compared: see the step log.\n\n' "$NAME" | summary
  exit 0
fi
echo "::endgroup::"

for side in redis kividb; do
  image=$REDIS_IMAGE
  [ "$side" = kividb ] && image=$KIVIDB_IMAGE
  result=$OUT/$NAME-$side.xml
  rm -f "$result"
  echo "::group::$NAME: against $side ($image)"
  if start_server "$image"; then
    bash "$HARNESS" run "$WORK" "$PORT" "$result" || echo "$NAME: no results against $side"
  else
    echo "$NAME: $side did not answer PING on port $PORT"
  fi
  docker logs "$CONTAINER" >"$OUT/$NAME-$side-server.log" 2>&1
  docker rm -f "$CONTAINER" >/dev/null 2>&1
  echo "::endgroup::"
done

[ -s "$OUT/$NAME-redis.xml" ] || echo "::warning title=$NAME::no results from the run against Redis, not compared"
python3 "$HERE/compare.py" "$NAME" "$OUT/$NAME-redis.xml" "$OUT/$NAME-kividb.xml" | summary
exit "${PIPESTATUS[0]}"
