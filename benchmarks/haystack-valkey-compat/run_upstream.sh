#!/bin/bash
# valkey-haystack 1.0.0's integration tests (`hatch run test:integration` = pytest -m integration tests)
# against one fresh server on localhost:6379, where the tests expect it.
# usage: run_upstream.sh LABEL IMAGE
#   e.g. run_upstream.sh valkey valkey/valkey-bundle:8.1
#        run_upstream.sh redis8 redis:8.10.2
#        run_upstream.sh kividb kividb/kividb:v1.2.0
# Setup used for the results here (Python 3.12):
#   git clone -b integrations/valkey-v1.0.0 https://github.com/deepset-ai/haystack-core-integrations hci
#   uv venv venv-hs && VIRTUAL_ENV=venv-hs uv pip install -e hci/integrations/valkey -r requirements.txt
set -u
LABEL=$1; IMAGE=$2; OUT=${OUT:-$PWD}
docker rm -f hs6379 >/dev/null 2>&1
case $IMAGE in
  redis:*)  docker run -d --name hs6379 --network host $IMAGE --port 6379 --save "" --appendonly no >/dev/null ;;
  valkey/*) docker run -d --name hs6379 --network host $IMAGE >/dev/null ;;
  *)        docker run -d --name hs6379 --network host $IMAGE --port 6379 --protected-mode no >/dev/null ;;
esac
sleep 3
redis-cli -p 6379 info server | grep -E "^redis_version|^valkey_version|kividb_version"
(cd hci/integrations/valkey && timeout 1800 ../../../venv-hs/bin/pytest -m integration tests -p no:cacheprovider \
  --junitxml=$OUT/upstream-tests-$LABEL.xml > $OUT/upstream-tests-$LABEL.log 2>&1)
echo "exit=$?"; tail -1 $OUT/upstream-tests-$LABEL.log
docker rm -f hs6379 >/dev/null
