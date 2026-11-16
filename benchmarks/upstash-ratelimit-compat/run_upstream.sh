#!/bin/bash
# @upstash/ratelimit's own test suite, through Serverless Redis HTTP (SRH 0.0.10, the open-source
# Upstash-REST-compatible proxy), against one fresh server.
# usage: ratelimit_upstream.sh LABEL IMAGE REF   (REF = git tag of ratelimit-js, e.g. v2.2.0)
# Run from a directory holding a clone of upstash/ratelimit-js in ./ratelimit-js. Versions behind the
# results here: bun 1.4.4, hiett/serverless-redis-http:0.0.10, @upstash/redis 1.34.3 for v2.0.8 (what
# `bun install` resolved). All three of the suite's "regions" point at the one SRH.
set -u
LABEL=$1; IMAGE=$2; REF=$3; PORT=7579; SRHPORT=8079
OUT=${OUT:-$PWD}
docker rm -f rl-$LABEL rl-srh >/dev/null 2>&1
case $IMAGE in
  redis:*) docker run -d --name rl-$LABEL --network host $IMAGE --port $PORT --save "" --appendonly no >/dev/null ;;
  *)       docker run -d --name rl-$LABEL --network host $IMAGE --port $PORT --protected-mode no >/dev/null ;;
esac
docker run -d --name rl-srh --network host -e SRH_MODE=env -e SRH_TOKEN=localtoken \
  -e SRH_CONNECTION_STRING=redis://127.0.0.1:$PORT -e SRH_PORT=$SRHPORT hiett/serverless-redis-http:0.0.10 >/dev/null
sleep 6
redis-cli -p $PORT info server | grep -E "^redis_version|kividb_version"
curl -s -H "Authorization: Bearer localtoken" -H "Content-Type: application/json" -d '["PING"]' http://127.0.0.1:$SRHPORT/ ; echo
cd ratelimit-js
git fetch -q --depth 1 origin tag $REF 2>/dev/null; git checkout -q -f $REF
bun install > $OUT/rl-install-$REF.log 2>&1
U=http://127.0.0.1:$SRHPORT
export UPSTASH_REDIS_REST_URL=$U UPSTASH_REDIS_REST_TOKEN=localtoken UPSTASH_DISABLE_TELEMETRY=1
for r in US1 EU2 APN; do export ${r}_UPSTASH_REDIS_REST_URL=$U ${r}_UPSTASH_REDIS_REST_TOKEN=localtoken; done
timeout 2400 bun test src --timeout 60000 > $OUT/ratelimit-$REF-$LABEL.log 2>&1
echo "ratelimit $REF exit=$?"; tail -6 $OUT/ratelimit-$REF-$LABEL.log
docker rm -f rl-$LABEL rl-srh >/dev/null
