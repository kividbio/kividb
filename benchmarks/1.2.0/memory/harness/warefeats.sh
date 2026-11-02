#!/bin/bash
# Runs the warefeats/kv-stores harness (pinned a324d29) unchanged, as its .github/workflows/kv-bench-cloud.yml
# "Run benchmark" step does, with ssh to the engine VM in place of AWS SSM. Client-side.
#   warefeats.sh <engine-ip> <memtier-threads> <smoke|full> <variant...>
# Variants: redis valkey dragonfly kividb-1.1.3 kividb-rc4 (anything after a further dash is a rerun label,
# e.g. kividb-rc4-run2). Each variant runs the full matrix on a fresh engine, then the random-bytes memory
# cell on another fresh engine. The engine compose files are warefeats' own (compose/cloud-*.yml); only the
# KiviDB image is swapped per variant.
set -uo pipefail
ENGINE_IP=$1; MEMTIER_THREADS=$2; MATRIX=$3; shift 3
VARIANTS="$*"
OUT=/var/tmp/wf; mkdir -p $OUT
ENG="ssh -i $HOME/.ssh/id_mem -o StrictHostKeyChecking=no -o ServerAliveInterval=30 azureuser@$ENGINE_IP"
SMOKE_FLAG=""; [ "$MATRIX" = smoke ] && SMOKE_FLAG="--smoke"
RC4_IMG=$($ENG cat /var/tmp/kividb-rc4-image)
ENGINE_THREADS=$($ENG nproc)

for V in $VARIANTS; do
  case $V in
    kividb-1.1.3*) ENGINE=kividb; IMG=kividb/kividb:v1.1.3 ;;
    kividb-rc4*) ENGINE=kividb; IMG=$RC4_IMG ;;
    *) ENGINE=${V%%-*}; IMG=$(grep -m1 '^\s*image:' /opt/bench/compose/docker-compose.${ENGINE}.yml | awk '{print $2}') ;;
  esac
  COMPOSE="-f compose/docker-compose.${ENGINE}.yml -f compose/cloud-overlay.yml -f compose/cloud-${ENGINE}.yml"
  if [ "$ENGINE" = kividb ]; then
    $ENG "printf 'services:\n  engine:\n    image: %s\n' '$IMG' > /opt/bench/compose/variant-$V.yml"
    COMPOSE="$COMPOSE -f compose/variant-$V.yml"
  fi
  engine_up() { $ENG "cd /opt/bench && sudo ENGINE_THREADS=$ENGINE_THREADS docker compose $COMPOSE -p kv-$ENGINE up -d engine --wait --wait-timeout 120"; }
  engine_down() {
    $ENG "cd /opt/bench && sudo docker compose $COMPOSE -p kv-$ENGINE logs --no-color engine" > $OUT/$V-$1-engine.log 2>&1
    $ENG "cd /opt/bench && sudo docker compose $COMPOSE -p kv-$ENGINE down -v --remove-orphans" || true
  }
  show_start() {
    $ENG "cd /opt/bench && for i in \$(seq 1 60); do redis-cli -p 6379 ping 2>/dev/null | grep -q PONG && break; sleep 1; done; sudo ENGINE_THREADS=$ENGINE_THREADS docker compose $COMPOSE -p kv-$ENGINE config | grep -E 'image:|seccomp|memlock|proactor' ; sudo docker compose $COMPOSE -p kv-$ENGINE logs --no-color engine | head -n 80; redis-cli -p 6379 INFO server | head -n 30" > $OUT/$V-$1-startup.log 2>&1
  }
  client_run() {
    (cd /opt/bench && ENGINE_HOST=$ENGINE_IP CLOUD_MODE=1 ENGINE_IMAGE=$IMG MEMTIER_THREADS=$MEMTIER_THREADS \
      MEMTIER_IMAGE=ghcr.io/warefeats/memtier-benchmark:2.5.1 \
      /usr/local/bin/bun run src/run.ts --engine=$ENGINE $SMOKE_FLAG --resume --output=$OUT/$V.json "$@")
  }
  echo "=== $V ($IMG) $(date -u +%FT%TZ) ==="
  engine_up; show_start matrix
  client_run
  engine_down matrix
  echo "--- $V random-bytes memory cell $(date -u +%FT%TZ) ---"
  engine_up; show_start random
  client_run --workload=memory-load-random
  engine_down random
done
echo "ALL DONE $(date -u +%FT%TZ)"
