#!/usr/bin/env bash
# Run the RAGFlow 0.27 harness (v0.27.3) against one server, from a clean stack. RAGFlow 1.0: run1.sh.
#
#   ./run.sh kividb       kividb/kividb:v1.2.0-lua
#   ./run.sh kividb-nolua kividb/kividb:v1.2.0
#   ./run.sh redis8       redis:8
#   ./run.sh valkey8      default          # RAGFlow's compose unchanged (valkey/valkey:8)
#
# Needs: Docker with Compose 2.35 or later, vm.max_map_count >= 262144 (Elasticsearch), a checkout
# of infiniflow/ragflow at the pinned tag in $RAGFLOW (default ~/ragflow), and a Python with
# requirements.txt installed in $PY (default python3). It deletes the stack's volumes before and
# after the run.
set -euo pipefail
LABEL=${1:?label}
IMAGE=${2:?image}
HERE=$(cd "$(dirname "$0")" && pwd)
RAGFLOW=${RAGFLOW:-$HOME/ragflow}
PY=${PY:-python3}
STUB=${STUB:-$HERE/../dify-compat/stub_openai.py}
cd "$RAGFLOW/docker"
# A project name of its own: Dify's compose also lives in a directory called docker.
export COMPOSE_PROJECT_NAME=ragflow

FILES=(-f docker-compose.yml)
case "$IMAGE" in
  default) ;;
  redis:*) FILES+=(-f "$HERE/compose.redis8.yaml"); export REDIS_IMAGE=$IMAGE ;;
  *)       FILES+=(-f "$HERE/compose.kividb.yaml"); export KIVIDB_IMAGE=$IMAGE ;;
esac
DC=(docker compose "${FILES[@]}")

clean() {
  "${DC[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
  docker rm -f stub-llm >/dev/null 2>&1 || true
}
clean

"${DC[@]}" up -d --quiet-pull
docker run -d --name stub-llm --network ragflow_ragflow -v "$(dirname "$STUB")":/h:ro python:3.12-slim \
  python /h/stub_openai.py --port 9999 >/dev/null

ARGS=(--label "$LABEL" --image "$IMAGE" --compose-dir "$RAGFLOW/docker" --ragflow-src "$RAGFLOW"
      --compose-file "$RAGFLOW/docker/docker-compose.yml")
[ "$IMAGE" = default ] || ARGS+=(--compose-file "${FILES[3]}")
set +e
"$PY" "$HERE/ragflow_kividb_compat.py" "${ARGS[@]}"
rc=$?
set -e
"${DC[@]}" logs --no-color > "$HERE/logs-$LABEL.txt" 2>&1 || true
[ -n "${KEEP:-}" ] || clean
exit $rc
