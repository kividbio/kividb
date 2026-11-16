#!/usr/bin/env bash
# Runs claude-mem's server job queue against KiviDB and against Redis 8, two ways each:
#
#   1. claude-mem's own Docker E2E (scripts/e2e-server-docker.sh), with the Valkey in its
#      docker-compose.yml swapped for the server under test;
#   2. claude_mem_queue_compat.mjs, which drives the same stack through enqueue, processing,
#      retry, failure, cancel, restarts and two workers, with a stub provider.
#
#   ./run.sh                 # both servers
#   ./run.sh kividb          # one
#
# Needs Docker with compose, Node 20 or newer, Bun 1.3 and git, on a throwaway Linux machine
# (it builds images and starts and stops containers). Writes results-<label>.json and
# e2e-<label>.txt next to this script.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLAUDE_MEM_REF="${CLAUDE_MEM_REF:-v15.9.0}"
WORK="${WORK:-$HERE/.work}"
SRC="$WORK/claude-mem"
LABELS=("$@")
[[ ${#LABELS[@]} -gt 0 ]] || LABELS=(kividb redis8 valkey)

export POSTGRES_USER=claudemem POSTGRES_PASSWORD=claudemem POSTGRES_DB=claudemem
export STUB_PROVIDER="$HERE/stub-provider.mjs"
export PATH="$HOME/.bun/bin:$PATH"

if [[ ! -d "$SRC" ]]; then
  mkdir -p "$WORK"
  git clone -q --depth 1 --branch "$CLAUDE_MEM_REF" https://github.com/thedotmack/claude-mem.git "$SRC"
fi
( cd "$SRC" && npm ci --no-audit --no-fund >/dev/null && npm run build >/dev/null )
( cd "$HERE" && npm ci --no-audit --no-fund >/dev/null )

for label in "${LABELS[@]}"; do
  backend="$HERE/compose.$label.yml"
  [[ -f "$backend" ]] || { echo "no $backend" >&2; exit 2; }

  echo "== $label: claude-mem's own Docker E2E"
  # The script's compose files, plus the one that swaps the Redis-protocol server.
  sed "s#^COMPOSE_FILES=(-f docker-compose.yml -f docker/e2e/docker-compose.e2e.yml)#COMPOSE_FILES=(-f docker-compose.yml -f docker/e2e/docker-compose.e2e.yml -f $backend)#" \
    "$SRC/scripts/e2e-server-docker.sh" > "$SRC/scripts/e2e-server-docker.kividb-compat.sh"
  grep -q -- "-f $backend" "$SRC/scripts/e2e-server-docker.kividb-compat.sh"
  if ( cd "$SRC" && COMPOSE_PROJECT_NAME="cme2e-$label" bash scripts/e2e-server-docker.kividb-compat.sh ) > "$HERE/e2e-$label.txt" 2>&1; then
    echo "   PASS (log: e2e-$label.txt)"
  else
    echo "   FAIL (log: e2e-$label.txt)"
  fi

  echo "== $label: queue harness"
  compose="docker compose -p cmq-$label --project-directory $SRC -f $SRC/docker-compose.yml -f $backend -f $HERE/compose.harness.yml"
  $compose down -v --remove-orphans >/dev/null 2>&1 || true
  $compose up -d --build postgres valkey claude-mem-server claude-mem-worker >/dev/null 2>&1
  set +e
  COMPOSE="$compose" LABEL="$label" CLAUDE_MEM_REF="$CLAUDE_MEM_REF" node "$HERE/claude_mem_queue_compat.mjs"
  rc=$?
  set -e
  [[ $rc -eq 0 ]] || $compose logs --no-color --tail=200 claude-mem-worker claude-mem-server valkey > "$HERE/harness-$label-failure.log" 2>&1 || true
  $compose down -v --remove-orphans >/dev/null 2>&1 || true
done
