#!/usr/bin/env bash
# CI entry point for this harness (the contract is in benchmarks/README.md).
#   ci.sh setup DIR              pulls the pinned redis_exporter image
#   ci.sh run DIR PORT OUT.xml   runs redis_exporter against 127.0.0.1:PORT, puts a known
#                                workload on the server, and checks what the exporter reports
# redis_exporter is what the KiviDB operator runs as its metrics sidecar, at the same version
# (kividb-operator's DefaultExporterImage). Any port works. The exporter listens on
# 127.0.0.1:19121, with host networking; EXPORTER_DOCKER_ARGS and EXPORTER_REDIS_HOST change
# that where there is no host networking (Docker Desktop: EXPORTER_DOCKER_ARGS="-p 19121:9121"
# EXPORTER_REDIS_HOST=host.docker.internal).
set -euo pipefail
here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
image=oliver006/redis_exporter:v1.66.0@sha256:d98e6db8094f491b95791e9f776b0ba30a20aeacb90e18334935d5e51bf2e6a1
cmd=${1:-} dir=${2:-}
case $cmd in
  setup) mkdir -p "$dir" && docker pull -q "$image" >/dev/null ;;
  run)
    port=$3 out=$4 name=compat-redis-exporter
    docker rm -f "$name" >/dev/null 2>&1 || true
    # shellcheck disable=SC2086 # EXPORTER_DOCKER_ARGS is a list of words
    docker run -d --name "$name" ${EXPORTER_DOCKER_ARGS:---network host} "$image" \
      --redis.addr="redis://${EXPORTER_REDIS_HOST:-127.0.0.1}:$port" \
      --web.listen-address="${EXPORTER_LISTEN:-127.0.0.1:19121}" >/dev/null
    trap 'docker logs "$name" >"$out.exporter.log" 2>&1; docker rm -f "$name" >/dev/null 2>&1' EXIT
    # The harness exits 0 whatever the results: they are in OUT.xml.
    timeout 120 python3 "$here/exporter_compat.py" --port "$port" --junitxml "$out" \
      --exporter-log-command "docker logs $name" || true
    test -s "$out"
    ;;
  *) echo "usage: ci.sh setup DIR | ci.sh run DIR PORT OUT.xml" >&2; exit 2 ;;
esac
