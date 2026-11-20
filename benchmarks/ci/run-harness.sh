#!/usr/bin/env bash
# run-harness.sh NAME: runs benchmarks/NAME-compat/ci.sh against Redis, then
# against KiviDB, and compares the results check by check (compare.py).
#
#   KIVIDB_IMAGE, REDIS_IMAGE  the servers, both pinned (required)
#   KIVIDB_LUA_IMAGE           the image for a harness that needs scripting (its folder has a
#                              `kividb-variant` file saying `lua`): default $KIVIDB_IMAGE-lua, the
#                              tag of the `lua` variant (release.yml); unchanged if it already ends in -lua or -full
#   OUT                        where the results go (default ./compat-results)
#   WORK                       where the harness sets itself up (default ./compat-work)
#   PORT                       the server's port (default 6379)
#
# Each server starts fresh, with host networking, on 127.0.0.1:PORT. The
# comparison goes to $GITHUB_STEP_SUMMARY when set, else to stdout. Exits 1
# when a check passes on Redis and fails on KiviDB, when a check the
# harness's kividb-passes.txt lists does not pass on KiviDB, or when the
# harness could not run: its setup failed, a server did not start, or a run
# left no results. The summary says which, so that a broken install is seen
# and is not read as KiviDB failing.
set -uo pipefail
NAME=$1
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
HARNESS=$(cd "$HERE/.." && pwd)/$NAME-compat/ci.sh
: "${KIVIDB_IMAGE:?set KIVIDB_IMAGE}" "${REDIS_IMAGE:?set REDIS_IMAGE}"
OUT=$(mkdir -p "${OUT:-compat-results}" && cd "${OUT:-compat-results}" && pwd)
WORK=$(mkdir -p "${WORK:-compat-work}/$NAME" && cd "${WORK:-compat-work}/$NAME" && pwd)
PORT=${PORT:-6379}
CONTAINER=compat-$NAME

# A harness whose checks need scripts (Lua) runs against the `lua` variant of the KiviDB image.
KIVIDB_RUN_IMAGE=$KIVIDB_IMAGE
if [ "$(cat "$(dirname "$HARNESS")/kividb-variant" 2>/dev/null)" = lua ]; then
  case $KIVIDB_IMAGE in
  *-lua | *-full) ;;
  *) KIVIDB_RUN_IMAGE=${KIVIDB_LUA_IMAGE:-$KIVIDB_IMAGE-lua} ;;
  esac
fi

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
  echo "::error title=$NAME::could not run: setup failed"
  printf '### %s\n\nCould not run: setup failed, see the step log.\n\n' "$NAME" | summary
  exit 1
fi
echo "::endgroup::"

down=()
for side in redis kividb; do
  image=$REDIS_IMAGE label=Redis
  [ "$side" = kividb ] && image=$KIVIDB_RUN_IMAGE label=KiviDB
  result=$OUT/$NAME-$side.xml
  rm -f "$result"
  echo "::group::$NAME: against $side ($image)"
  if start_server "$image"; then
    bash "$HARNESS" run "$WORK" "$PORT" "$result" || echo "$NAME: no results against $side"
  else
    echo "$NAME: $side did not answer PING on port $PORT"
    down+=("$label")
  fi
  docker logs "$CONTAINER" >"$OUT/$NAME-$side-server.log" 2>&1
  docker rm -f "$CONTAINER" >/dev/null 2>&1
  echo "::endgroup::"
done

# A server that did not start: Redis's is this harness's problem, KiviDB's
# is KiviDB's; either way there is nothing to compare.
if [ ${#down[@]} -gt 0 ]; then
  who=${down[0]}
  [ ${#down[@]} -gt 1 ] && who="${down[0]} and ${down[1]}"
  echo "::error title=$NAME::$who did not start: no answer to PING on port $PORT"
  printf '### %s\n\n%s did not start: no answer to PING on port %s, see the step log and the server log.\n\n' \
    "$NAME" "$who" "$PORT" | summary
  exit 1
fi

passes=$(cd "$HERE/.." && pwd)/$NAME-compat/kividb-passes.txt
args=("$NAME" "$OUT/$NAME-redis.xml" "$OUT/$NAME-kividb.xml")
[ -f "$passes" ] && args+=("$passes")
python3 "$HERE/compare.py" "${args[@]}" | summary
status=${PIPESTATUS[0]}
[ "$status" = 0 ] || echo "::error title=$NAME::see the job summary"
exit "$status"
