#!/usr/bin/env bash
# Firecrawl v2.11.0 from its own docker-compose.yaml, then firecrawl_e2e.py against it (with the
# server restarted mid-crawl).
#
#   FC=~/firecrawl ./run_compose.sh kividb    # with docker-compose.kividb.yaml, REDIS_URL=redis://kividb:6380
#   FC=~/firecrawl ./run_compose.sh redis     # Firecrawl's compose file as it is, with its own redis service
#
# FC is a checkout of firecrawl/firecrawl at v2.11.0 with the images built (docker compose build api
# playwright-service nuq-postgres) and the test site built (apps/test-site: pnpm install && pnpm build).
# Writes e2e-compose-<mode>.json next to this script.
set -euo pipefail
MODE=${1:?kividb or redis}
HERE=$(cd "$(dirname "$0")" && pwd)
FC=${FC:?set FC to the firecrawl checkout}
cd "$FC"
cp "$HERE/docker-compose.kividb.yaml" "$HERE/docker-compose.test.yaml" .
printf 'USE_DB_AUTHENTICATION=false\nALLOW_LOCAL_WEBHOOKS=true\n' > .env
if [ "$MODE" = kividb ]; then
  echo 'REDIS_URL=redis://kividb:6380' >> .env
  PORT=6380
else
  PORT=6379
fi
F=(-f docker-compose.yaml -f docker-compose.kividb.yaml -f docker-compose.test.yaml)

cleanup() {
  docker rm -f fc-test-site >/dev/null 2>&1 || true
  docker compose "${F[@]}" down -v >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker compose "${F[@]}" up -d
# The built test site, served as static files on Firecrawl's network and on the host (127.0.0.1:4321).
# Firecrawl accepts only URLs with a public-looking host name, so the site gets a network alias under
# example.org, which Docker's DNS answers inside that network.
docker run -d --name fc-test-site --network firecrawl_backend --network-alias test-site.example.org \
  -p 127.0.0.1:4321:4321 -v "$FC/apps/test-site/dist:/site:ro" \
  python:3.12-slim python -m http.server 4321 -d /site >/dev/null
until curl -s -o /dev/null http://127.0.0.1:3002/; do sleep 2; done
sleep 10
docker compose "${F[@]}" exec -T api sh -c 'echo "REDIS_URL=$REDIS_URL REDIS_RATE_LIMIT_URL=$REDIS_RATE_LIMIT_URL"'

FC_LABEL=compose-$MODE FC_REDIS_PORT=$PORT FC_SERVER_CONTAINER=fc-compose-$MODE \
  TEST_SUITE_WEBSITE=http://test-site.example.org:4321 python3 "$HERE/firecrawl_e2e.py"
if [ "$MODE" = kividb ]; then
  echo "the unused redis service's connections (1 is redis-cli's own):"
  docker compose "${F[@]}" exec -T redis redis-cli info clients | grep connected_clients
fi
