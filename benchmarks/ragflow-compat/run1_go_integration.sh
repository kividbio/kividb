#!/usr/bin/env bash
# RAGFlow's own Go integration tests that talk to a real Kvrocks, run against each server standalone:
#   $RAGFLOW (v1.0.0-rc3):   ./internal/engine/kvrocks     TestKvrocksInitConnects uses KVROCKS_TEST_ADDR
#   $RAGFLOW_MAIN (main):    ./internal/ingestion/mediacache  TestStoreRealKvrocksLifecycle uses
#                            RAGFLOW_TEST_KVROCKS_ADDR (added after v1.0.0-rc3)
# The other tests in those packages use an in-process miniredis. Runs in golang:1.27 containers.
set -u
RAGFLOW=${RAGFLOW:-$HOME/ragflow}
RAGFLOW_MAIN=${RAGFLOW_MAIN:-$HOME/ragflow-main}
PW=infini_rag_flow
docker rm -f it-kvrocks it-kividb it-kividb-nolua >/dev/null 2>&1
# Kvrocks the way RAGFlow's compose starts it (its entrypoint script renders the password into the config).
docker run -d --name it-kvrocks -p 17001:6379 -u 0 -e REDIS_PASSWORD=$PW -v "$RAGFLOW/docker/kvrocks-entrypoint.sh":/e.sh:ro \
  --entrypoint /bin/sh apache/kvrocks:2.16.0 /e.sh >/dev/null
docker run -d --name it-kividb -p 17002:6379 kividb/kividb:v1.2.0-lua --port 6379 --requirepass $PW >/dev/null
docker run -d --name it-kividb-nolua -p 17003:6379 kividb/kividb:v1.2.0 --port 6379 --requirepass $PW >/dev/null
sleep 3
for pair in kvrocks:17001 kividb:17002 kividb-nolua:17003; do
  n=${pair%%:*}; p=${pair##*:}
  for spec in "$RAGFLOW ./internal/engine/kvrocks/ KVROCKS_TEST_ADDR" "$RAGFLOW_MAIN ./internal/ingestion/mediacache/ RAGFLOW_TEST_KVROCKS_ADDR"; do
    set -- $spec
    echo "##### $n $(git -C "$1" describe --tags --always) $2"
    docker run --rm --network host -v "$1":/src -v ragflow-gomod:/go/pkg/mod -v ragflow-gocache:/root/.cache/go-build -w /src \
      -e CGO_ENABLED=0 -e "$3=127.0.0.1:$p" -e REDIS_PASSWORD=$PW golang:1.27 \
      go test -tags integration -count=1 -timeout 15m -v "$2" 2>&1 | grep -E '^(=== RUN|--- |PASS|FAIL|ok|panic)|_test.go:[0-9]+:'
  done
done
docker rm -f it-kvrocks it-kividb it-kividb-nolua >/dev/null 2>&1
