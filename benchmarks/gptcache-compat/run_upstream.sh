#!/bin/bash
# As run on the test VM: clones and venvs under ~/fw (see the setup notes in the docs page).
# GPTCache 0.1.44's Redis tests against one fresh server on localhost:6379 (the tests' fixed URL).
# usage: gptcache_upstream.sh LABEL IMAGE
set -u
LABEL=$1; IMAGE=$2; OUT=~/fw/out
docker rm -f gc6379 >/dev/null 2>&1
case $IMAGE in
  redis:*)  docker run -d --name gc6379 --network host $IMAGE --port 6379 --save "" --appendonly no >/dev/null ;;
  valkey/*) docker run -d --name gc6379 --network host $IMAGE >/dev/null ;;
  *)        docker run -d --name gc6379 --network host $IMAGE --port 6379 --protected-mode no >/dev/null ;;
esac
sleep 3
redis-cli -p 6379 info server | grep -E "^redis_version|^valkey_version|kividb_version"
cd ~/fw/gptcache
# GPTCache installs what a test imports with `pip` on the fly; those were installed into the venv first.
PATH=$HOME/fw/venv-gc/bin:$PATH timeout 1800 ~/fw/venv-gc/bin/pytest tests/unit_tests/manager/test_redis.py \
  tests/unit_tests/manager/test_redis_cache_storage.py tests/unit_tests/eviction/test_distributed_cache.py \
  tests/integration_tests/test_redis_onnx.py -p no:cacheprovider -o addopts="" \
  --junitxml=$OUT/gptcache-$LABEL.xml > $OUT/gptcache-$LABEL.log 2>&1
echo "gptcache exit=$?"; tail -1 $OUT/gptcache-$LABEL.log
docker rm -f gc6379 >/dev/null
