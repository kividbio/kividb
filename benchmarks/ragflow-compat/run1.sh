#!/usr/bin/env bash
# Run the RAGFlow 1.0 harness, then RAGFlow's own Go HTTP API suite, against one server in the
# `kvrocks` role, each from a clean stack.
#
#   ./run1.sh kvrocks      default                    # RAGFlow's compose unchanged (apache/kvrocks:2.16.0)
#   ./run1.sh kividb       kividb/kividb:v1.2.0-lua
#   ./run1.sh kividb-nolua kividb/kividb:v1.2.0
#   ./run1.sh redis8       redis:8
#
# Needs: Docker with Compose 2.24 or later, vm.max_map_count >= 262144 (Elasticsearch), a checkout
# of infiniflow/ragflow at the pinned tag in $RAGFLOW (default ~/ragflow), and a Python with
# requirements.txt installed in $PY (default python3). The Go suite runs in a golang:1.27
# container on the host network (SUITE=0 skips it). It deletes the stack's volumes before, between
# and after the runs.
set -euo pipefail
LABEL=${1:?label}
IMAGE=${2:?image}
HERE=$(cd "$(dirname "$0")" && pwd)
RAGFLOW=${RAGFLOW:-$HOME/ragflow}
PY=${PY:-python3}
STUB=${STUB:-$HERE/../dify-compat/stub_openai.py}
SUITE=${SUITE:-1}
cd "$RAGFLOW/docker"
export COMPOSE_PROJECT_NAME=ragflow

FILES=(-f docker-compose.yml)
case "$IMAGE" in
  default) ;;
  redis:*) FILES+=(-f "$HERE/compose.redis8-kvrocks.yaml"); export REDIS_IMAGE=$IMAGE ;;
  *)       FILES+=(-f "$HERE/compose.kividb-kvrocks.yaml"); export KIVIDB_IMAGE=$IMAGE ;;
esac
DC=(docker compose "${FILES[@]}")
ARGS=(--label "$LABEL" --image "$IMAGE" --compose-dir "$RAGFLOW/docker" --ragflow-src "$RAGFLOW"
      --compose-file "$RAGFLOW/docker/docker-compose.yml")
[ "$IMAGE" = default ] || ARGS+=(--compose-file "${FILES[3]}")

clean() {
  "${DC[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
  docker rm -f stub-llm >/dev/null 2>&1 || true
}
up() {
  "${DC[@]}" up -d --quiet-pull
  docker run -d --name stub-llm --network ragflow_ragflow -v "$(dirname "$STUB")":/h:ro python:3.12-slim \
    python /h/stub_openai.py --port 9999 >/dev/null
}

clean
up
set +e
"$PY" "$HERE/ragflow1_kividb_compat.py" "${ARGS[@]}"
rc=$?
set -e
"${DC[@]}" logs --no-color > "$HERE/logs-1.0-$LABEL.txt" 2>&1 || true

if [ "$SUITE" = 1 ]; then
  # RAGFlow's own HTTP API suite (test/api, Go, `integration` tag), on a fresh stack. It registers
  # the providers it has keys for; with none, the stub is made the tenant's default first, so the
  # tests that need a chat or embedding model run against it.
  clean
  up
  "$PY" "$HERE/ragflow1_kividb_compat.py" "${ARGS[@]}" --prepare
  docker volume create ragflow-gomod >/dev/null
  docker volume create ragflow-gocache >/dev/null
  set +e
  docker run --rm --network host -v "$RAGFLOW":/src -v ragflow-gomod:/go/pkg/mod -v ragflow-gocache:/root/.cache/go-build \
    -w /src -e HOST_ADDRESS=http://127.0.0.1:9380 -e CGO_ENABLED=0 golang:1.27 \
    go test -tags integration -count=1 -timeout 60m -json ./test/api > "$HERE/go-suite-$LABEL.raw.json"
  set -e
  "$PY" "$HERE/go_suite_summary.py" "$HERE/go-suite-$LABEL.raw.json" "$HERE/suite-1.0-$LABEL.json" "$LABEL" "$IMAGE"
  rm -f "$HERE/go-suite-$LABEL.raw.json"
  "${DC[@]}" logs --no-color > "$HERE/logs-1.0-suite-$LABEL.txt" 2>&1 || true
fi
[ -n "${KEEP:-}" ] || clean
exit $rc
