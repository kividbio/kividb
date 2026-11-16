#!/usr/bin/env bash
# Run the Dify harness against one server, from a clean stack.
#
#   ./run.sh kividb      kividb/kividb:v1.2.0-lua
#   ./run.sh kividb-nolua kividb/kividb:v1.2.0
#   ./run.sh redis8      redis:8
#
# Needs: Docker with Compose 2.35 or later, a checkout of langgenius/dify at the pinned tag in
# $DIFY (default ~/dify), and a Python with requirements.txt installed in $PY (default python3).
# It deletes the stack's data (docker/volumes) before and after the run.
set -euo pipefail
LABEL=${1:?label}
IMAGE=${2:?image}
HERE=$(cd "$(dirname "$0")" && pwd)
DIFY=${DIFY:-$HOME/dify}
PY=${PY:-python3}
cd "$DIFY/docker"
# A project name of its own: RAGFlow's compose also lives in a directory called docker.
export COMPOSE_PROJECT_NAME=dify

case "$IMAGE" in
  redis:*) OVERRIDE=$HERE/compose.redis8.yaml; export REDIS_IMAGE=$IMAGE ;;
  *)       OVERRIDE=$HERE/compose.kividb.yaml; export KIVIDB_IMAGE=$IMAGE ;;
esac
DC=(docker compose -f docker-compose.yaml -f "$OVERRIDE")

clean() {
  "${DC[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
  docker rm -f stub-llm >/dev/null 2>&1 || true
  sudo git clean -fdxq volumes
  git checkout -q -- volumes
}
clean

cp .env.example .env
# Dify's agent backend and its local sandbox refuse to start without a shared token.
echo "DIFY_AGENT_LOCAL_SANDBOX_AUTH_TOKEN=$(openssl rand -hex 16)" >> .env
mkdir -p volumes/kividb/data && sudo chown 1000:1000 volumes/kividb/data
# The plugin daemon runs as uid 1001 and cannot create its storage in a directory Docker made as
# root (the same on any server): make it first.
mkdir -p volumes/plugin_daemon && sudo chown 1001:1001 volumes/plugin_daemon

"${DC[@]}" up -d --quiet-pull
docker run -d --name stub-llm --network dify_default -v "$HERE":/h:ro python:3.12-slim \
  python /h/stub_openai.py --port 9999 >/dev/null

set +e
"$PY" "$HERE/dify_kividb_compat.py" --label "$LABEL" --image "$IMAGE" --compose-dir "$DIFY/docker" \
  --compose-file "$DIFY/docker/docker-compose.yaml" --compose-file "$OVERRIDE" --dify-src "$DIFY" \
  --stub-url http://stub-llm:9999/v1
rc=$?
set -e
"${DC[@]}" logs --no-color > "$HERE/logs-$LABEL.txt" 2>&1 || true
[ -n "${KEEP:-}" ] || clean
exit $rc
