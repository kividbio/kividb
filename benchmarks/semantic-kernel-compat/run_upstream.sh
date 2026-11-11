#!/bin/bash
# semantic-kernel 1.47.0's own Redis tests (the four Redis cases of the vector store integration test)
# against one fresh server on localhost:6379.
# usage: run_upstream.sh LABEL IMAGE      e.g. run_upstream.sh kividb kividb/kividb:v1.2.0
# Setup used for the results here:
#   git clone -b python-1.47.0 https://github.com/microsoft/semantic-kernel
#   cd semantic-kernel/python && uv sync --frozen --python 3.12 --extra redis --group dev && uv pip install pandas
set -u
LABEL=$1; IMAGE=$2; OUT=${OUT:-$PWD}
docker rm -f sk6379 >/dev/null 2>&1
case $IMAGE in
  redis:*) docker run -d --name sk6379 --network host $IMAGE --port 6379 --save "" --appendonly no >/dev/null ;;
  *)       docker run -d --name sk6379 --network host $IMAGE --port 6379 --protected-mode no >/dev/null ;;
esac
sleep 3
redis-cli -p 6379 info server | grep -E "^redis_version|kividb_version"
(cd semantic-kernel/python && REDIS_CONNECTION_STRING=redis://localhost:6379 timeout 1800 \
  uv run --no-sync pytest tests/integration/memory/test_vector_store.py -k redis -p no:cacheprovider \
  --junitxml=$OUT/upstream-tests-$LABEL.xml > $OUT/upstream-tests-$LABEL.log 2>&1)
echo "exit=$?"; tail -1 $OUT/upstream-tests-$LABEL.log
docker rm -f sk6379 >/dev/null
