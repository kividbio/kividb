# shellcheck shell=bash
# Engines for the regression suite: fetch, check, start and stop.
#
# Source this file; it defines functions and changes no shell options.
#
#   engines_fetch_release <tag> <variant> <dir>  -> prints the binary's path
#        Downloads kividb[-<variant>]-linux-<arch>.tar.gz of <tag> and the
#        release's SHA256SUMS.txt (or reuses a cached copy that still matches
#        it), checks the tarball against it, and unpacks the binary afresh.
#        compare.py also checks the v1.2.2 binary's SHA-256 against the pin in
#        baseline.json.
#        <variant> is "default" (glibc, no Lua) or one of full, lua, tls.
#   engines_resolve_candidate <path|tag> <dir>   -> prints the binary's path
#        An existing file is used as it is; anything else is taken as a tag.
#   engines_resolve_redis <dir>                  -> prints redis-server's path
#        $REDIS_SERVER, else /opt/redis-$REDIS_VERSION/src/redis-server, else
#        built from the $REDIS_VERSION tag of github.com/redis/redis into <dir>.
#        The version must start with $REDIS_SERIES (8.10.).
#   engine_start <kind> <bin> <io_model> <threads> [extra args...]
#        <kind> is kividb or redis. Picks a free non-ephemeral port, starts the
#        server in a new empty directory, waits for +PONG (30 s at most) and
#        sets ENGINE_PORT, ENGINE_PID, ENGINE_DIR, ENGINE_LOG. io_model is
#        auto, tokio or uring; threads 0 is the server's default. Both are
#        ignored for Redis, which runs with its defaults (io-threads 1).
#   engine_stop
#        SHUTDOWN NOSAVE, then SIGTERM after 10 s, then SIGKILL. Safe to call
#        twice, and from an EXIT trap.
#
# Settings (environment): REGRESS_CACHE (default ~/.cache/kividb-regress),
# REDIS_VERSION (8.10.2), REDIS_SERIES (8.10.), PYTHON (python3),
# KIVIDB_REPO (kividbio/kividb).

REGRESS_LIB=${REGRESS_LIB:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}
REGRESS_CACHE=${REGRESS_CACHE:-$HOME/.cache/kividb-regress}
REDIS_VERSION=${REDIS_VERSION:-8.10.2}
REDIS_SERIES=${REDIS_SERIES:-8.10.}
KIVIDB_REPO=${KIVIDB_REPO:-kividbio/kividb}
PYTHON=${PYTHON:-python3}

ENGINE_PORT= ENGINE_PID= ENGINE_DIR= ENGINE_LOG= ENGINE_KIND=

engines_log() { echo "[engines] $*" >&2; }

engines_arch() {
  case $(uname -m) in
    x86_64 | amd64) echo x86_64 ;;
    aarch64 | arm64) echo aarch64 ;;
    *) engines_log "unsupported machine $(uname -m)"; return 1 ;;
  esac
}

engines_sum_line() { # SHA256SUMS.txt name -> its line as "<sum>  <name>"; fails if absent
  awk -v n="$2" '{ f = $2; sub(/^\*/, "", f); sub(/^\.\//, "", f); if (f == n) { print $1 "  " n; found = 1 } } END { exit !found }' "$1"
}

engines_fetch_release() {
  local tag=$1 variant=$2 dir=$3 arch name base out sums
  arch=$(engines_arch) || return 1
  if [ "$variant" = default ]; then name=kividb-linux-$arch.tar.gz; else name=kividb-$variant-linux-$arch.tar.gz; fi
  out=$dir/$tag/$variant
  sums=$dir/$tag/SHA256SUMS.txt
  mkdir -p "$out"
  base=https://github.com/$KIVIDB_REPO/releases/download/$tag
  # A cached tarball is used only if it still matches the release's SHA256SUMS.txt;
  # the binary is extracted from it afresh on every run, never trusted from the cache.
  if ! { [ -f "$sums" ] && [ -f "$dir/$tag/$name" ] &&
    engines_sum_line "$sums" "$name" | (cd "$dir/$tag" && sha256sum -c --status -); }; then
    engines_log "fetching $name of $tag"
    curl -fsSL --retry 3 -o "$sums" "$base/SHA256SUMS.txt" || { engines_log "no SHA256SUMS.txt for $tag"; return 1; }
    curl -fsSL --retry 3 -o "$dir/$tag/$name" "$base/$name" || { engines_log "no $name for $tag"; return 1; }
    # Check exactly the one file we use; a missing line is a failure, not a pass.
    if ! engines_sum_line "$sums" "$name" | (cd "$dir/$tag" && sha256sum -c --status -); then
      engines_log "SHA-256 of $name does not match SHA256SUMS.txt of $tag"
      rm -f "$dir/$tag/$name"
      return 1
    fi
  fi
  rm -rf "$out/pkg" "$out/kividb" "$out/.verified"
  mkdir -p "$out/pkg"
  tar -xzf "$dir/$tag/$name" -C "$out/pkg"
  local bin
  bin=$(find "$out/pkg" -type f -name kividb -perm -u+x | head -1)
  [ -n "$bin" ] || { engines_log "no kividb binary inside $name"; return 1; }
  ln -s "$bin" "$out/kividb"
  echo "$out/kividb"
}

engines_resolve_candidate() {
  local what=$1 dir=$2
  if [ -f "$what" ]; then
    [ -x "$what" ] || { engines_log "$what is not executable"; return 1; }
    (cd "$(dirname "$what")" && echo "$(pwd)/$(basename "$what")")
  else
    engines_fetch_release "$what" default "$dir"
  fi
}

engines_redis_ok() { # bin -> 0 if its version is in REDIS_SERIES
  local v
  v=$("$1" --version 2>/dev/null | sed -n 's/.* v=\([0-9.]*\).*/\1/p')
  case $v in "$REDIS_SERIES"*) return 0 ;; esac
  engines_log "$1 is Redis '$v', not $REDIS_SERIES*"
  return 1
}

engines_resolve_redis() {
  local dir=$1 bin
  for bin in "${REDIS_SERVER:-}" "/opt/redis-$REDIS_VERSION/src/redis-server" "$dir/redis-$REDIS_VERSION/src/redis-server"; do
    if [ -n "$bin" ] && [ -x "$bin" ] && engines_redis_ok "$bin"; then echo "$bin"; return 0; fi
  done
  engines_log "building Redis $REDIS_VERSION from its tag (a few minutes)"
  mkdir -p "$dir"
  rm -rf "$dir/redis-$REDIS_VERSION"
  git clone -q --depth 1 --branch "$REDIS_VERSION" https://github.com/redis/redis.git "$dir/redis-$REDIS_VERSION" >&2 || return 1
  make -C "$dir/redis-$REDIS_VERSION" -j"$(nproc)" BUILD_TLS=no >"$dir/redis-build.log" 2>&1 || { engines_log "Redis build failed: $dir/redis-build.log"; return 1; }
  bin=$dir/redis-$REDIS_VERSION/src/redis-server
  engines_redis_ok "$bin" && echo "$bin"
}

engines_free_port() {
  "$PYTHON" "$REGRESS_LIB/ports.py"
}

engines_ping() { # port -> 0 on +PONG
  local reply
  { exec 9<>"/dev/tcp/127.0.0.1/$1"; } 2>/dev/null || return 1
  printf 'PING\r\n' >&9
  IFS= read -r -t 2 reply <&9 || reply=
  exec 9>&- 9<&-
  [ "${reply%$'\r'}" = "+PONG" ]
}

engines_send() { # port cmd... (inline command, reply ignored)
  local port=$1; shift
  { exec 9<>"/dev/tcp/127.0.0.1/$port"; } 2>/dev/null || return 1
  printf '%s\r\n' "$*" >&9
  IFS= read -r -t 5 _ <&9 || true
  exec 9>&- 9<&-
}

engine_start() {
  local kind=$1 bin=$2 io=$3 threads=$4; shift 4
  engine_stop
  ENGINE_KIND=$kind
  ENGINE_DIR=$(mktemp -d "${TMPDIR:-/tmp}/kividb-regress.XXXXXX")
  ENGINE_LOG=$ENGINE_DIR/server.log
  ENGINE_PORT=$(engines_free_port) || return 1
  case $kind in
    redis)
      "$bin" --port "$ENGINE_PORT" --bind 127.0.0.1 --save '' --appendonly no \
        --enable-debug-command yes --dir "$ENGINE_DIR" "$@" </dev/null >"$ENGINE_LOG" 2>&1 &
      ;;
    kividb)
      local args=(--port "$ENGINE_PORT" --bind 127.0.0.1 --protected-mode no --dir "$ENGINE_DIR")
      [ "$io" != auto ] && args+=(--io-model "$io")
      [ "${threads:-0}" != 0 ] && args+=(--threads "$threads")
      (cd "$ENGINE_DIR" && KIVIDB_NO_UPDATE_CHECK=1 exec "$bin" "${args[@]}" "$@") </dev/null >"$ENGINE_LOG" 2>&1 &
      ;;
    *) engines_log "unknown engine kind $kind"; return 1 ;;
  esac
  ENGINE_PID=$!
  local i
  for i in $(seq 300); do
    if engines_ping "$ENGINE_PORT"; then return 0; fi
    if ! kill -0 "$ENGINE_PID" 2>/dev/null; then break; fi
    sleep 0.1
  done
  engines_log "$kind did not answer PING on port $ENGINE_PORT; log:"
  tail -20 "$ENGINE_LOG" >&2
  engine_stop
  return 1
}

engine_stop() {
  [ -n "$ENGINE_PID" ] || return 0
  if kill -0 "$ENGINE_PID" 2>/dev/null; then
    engines_send "$ENGINE_PORT" SHUTDOWN NOSAVE 2>/dev/null || true
    local i
    for i in $(seq 100); do kill -0 "$ENGINE_PID" 2>/dev/null || break; sleep 0.1; done
    kill -TERM "$ENGINE_PID" 2>/dev/null || true
    for i in $(seq 50); do kill -0 "$ENGINE_PID" 2>/dev/null || break; sleep 0.1; done
    kill -KILL "$ENGINE_PID" 2>/dev/null || true
  fi
  wait "$ENGINE_PID" 2>/dev/null || true
  [ -n "$ENGINE_DIR" ] && [ -d "$ENGINE_DIR" ] && rm -rf "$ENGINE_DIR"
  ENGINE_PID= ENGINE_PORT= ENGINE_DIR=
}
