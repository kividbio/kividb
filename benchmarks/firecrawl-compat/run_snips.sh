#!/usr/bin/env bash
# Firecrawl's own self-hosted test suite against one Redis-protocol server.
#
#   FC=~/firecrawl ./run_snips.sh kividb      # kividb/kividb:v1.2.0-lua on 6380
#   FC=~/firecrawl ./run_snips.sh redis8      # redis:8 on 6379
#
# It does what the "Self-hosted environment tests" job of Firecrawl's CI
# (.github/workflows/test-server.yml at v2.11.0) does, in its fetch / no-proxy matrix cell and
# with no AI key and no search engine: Postgres for the NuQ queue, RabbitMQ, the local test site
# (apps/test-site) on 127.0.0.1:4321, then `pnpm harness pnpm test:snips` in apps/api, which starts
# the API and every worker and runs src/__tests__/snips/v1 and v2. Tests that need an AI key, a
# search engine, Playwright, a proxy or Firecrawl's cloud services skip themselves.
#
# FC is a checkout of firecrawl/firecrawl at v2.11.0 with the CI's build steps done: the native lib
# (apps/api/native: pnpm install), the Go html-to-markdown lib, `pnpm install --frozen-lockfile
# --ignore-scripts` in apps/api and the test site installed and built. EXTRA_CMD, if set, runs
# under the same harness after the suite (firecrawl_e2e.py uses it).
#
# Writes junit-<label>.xml and commandstats-<label>.txt next to this script.
set -euo pipefail
LABEL=$1
HERE=$(cd "$(dirname "$0")" && pwd)
FC=${FC:?set FC to the firecrawl checkout}

case $LABEL in
  kividb*) IMG=${IMG:-kividb/kividb:v1.2.0-lua}; PORT=6380
           RUN=(--security-opt seccomp=unconfined -v fc-data:/home/kividb "$IMG" --protected-mode no --aof) ;;
  redis8*) IMG=${IMG:-redis:8}; PORT=6379
           RUN=(-v fc-data:/data "$IMG" redis-server --appendonly yes --dir /data) ;;
  *) echo "label must start with kividb or redis8" >&2; exit 2 ;;
esac

cleanup() {
  docker rm -f fc-redis fc-rabbit fc-pg >/dev/null 2>&1 || true
  docker volume rm -f fc-data >/dev/null 2>&1 || true
  [ -n "${SITE_PID:-}" ] && kill "$SITE_PID" 2>/dev/null || true
}
trap cleanup EXIT
cleanup

docker run -d --name fc-redis -p 127.0.0.1:$PORT:$PORT "${RUN[@]}" >/dev/null
docker run -d --name fc-rabbit -p 127.0.0.1:5672:5672 rabbitmq:3 >/dev/null
docker run -d --name fc-pg -p 127.0.0.1:5432:5432 -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=postgres firecrawl/nuq-postgres:latest >/dev/null

(cd "$FC/apps/test-site" && exec pnpm preview --port 4321 --strictPort --host 127.0.0.1 > "$HERE/test-site.log" 2>&1) &
SITE_PID=$!
for p in 4321 5672 5432 $PORT; do
  for i in $(seq 1 60); do (echo > /dev/tcp/127.0.0.1/$p) 2>/dev/null && break; sleep 1; done
done
sleep 5  # RabbitMQ accepts TCP before it accepts AMQP logins

export HOST=0.0.0.0
export TEST_SUITE_SELF_HOSTED=true
export TEST_SUITE_WEBSITE=http://127.0.0.1:4321
export REDIS_URL=redis://127.0.0.1:$PORT
export REDIS_RATE_LIMIT_URL=redis://127.0.0.1:$PORT
export NUQ_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres
export NUQ_RABBITMQ_URL=amqp://localhost:5672
export USE_GO_MARKDOWN_PARSER=true
export ALLOW_LOCAL_WEBHOOKS=true
export FIRECRAWL_LOG_TO_FILE=true
export npm_config_ignore_scripts=true
export FC_REDIS_PORT=$PORT FC_LABEL=$LABEL

cd "$FC/apps/api"
rm -f test-results/junit.xml
set +e
# The harness can start the suite before the API listens (it was 5 s early here, and the first
# test files failed with ECONNREFUSED on both servers), so wait for the API first.
WAIT_API='until curl -s -o /dev/null http://127.0.0.1:3002/; do sleep 1; done; sleep 3'
# SUITE overrides the suite command, for example to rerun some files:
#   SUITE="pnpm exec vitest run src/__tests__/snips/v2/crawl.test.ts"
pnpm harness bash -c "$WAIT_API; ${SUITE:-pnpm test:snips}; rc=\$?; ${EXTRA_CMD:-true}; exit \$rc"
rc=$?
set -e
cp test-results/junit.xml "$HERE/junit-$LABEL.xml" || true
redis-cli -p "$PORT" info commandstats > "$HERE/commandstats-$LABEL.txt" || true
echo "suite exit code $rc"
