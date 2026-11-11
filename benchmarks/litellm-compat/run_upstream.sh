#!/bin/bash
# As run on the test VM: clones and venvs under ~/fw (see the setup notes in the docs page).
# LiteLLM v1.108.0's own Redis tests (the `sdk` integration group's Redis files) against one server.
# usage: litellm_upstream.sh LABEL IMAGE PATHDIR
#   IMAGE   docker image for the shared server (REDIS_HOST/REDIS_PORT)
#   PATHDIR dir holding the `redis-server` the suite spawns for tests that own a Redis
set -u
LABEL=$1; IMAGE=$2; PATHDIR=$3
PORT=7379
cd ~/fw/litellm
docker rm -f ll-$LABEL >/dev/null 2>&1
case $IMAGE in
  redis:*) docker run -d --name ll-$LABEL --network host $IMAGE --port $PORT --save "" --appendonly no >/dev/null ;;
  *)       docker run -d --name ll-$LABEL --network host $IMAGE --port $PORT --protected-mode no >/dev/null ;;
esac
sleep 2
redis-cli -p $PORT info server | grep -E "redis_version|kividb_version"
FILES=$(grep -liE "redis" tests/integration/sdk/test_*.py | tr "\n" " ")
export PATH=$HOME/fw/$PATHDIR:$HOME/.local/bin:$PATH
which redis-server
export REDIS_HOST=127.0.0.1 REDIS_PORT=$PORT LITELLM_LOCAL_MODEL_COST_MAP=True
timeout 3000 uv run --no-sync python tests/integration/run.py sdk --results ~/fw/out/litellm-$LABEL $FILES > ~/fw/out/litellm-$LABEL.log 2>&1
echo "exit=$?"
docker rm -f ll-$LABEL >/dev/null
tail -5 ~/fw/out/litellm-$LABEL.log
