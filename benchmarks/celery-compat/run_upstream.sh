#!/bin/bash
# As run on the test VM: clones and venvs under ~/fw (see the setup notes in the docs page).
# celery 5.7.0's and kombu 5.7.0's own Redis integration suites against one fresh server.
# usage: celery_upstream.sh LABEL IMAGE [SUITES]
#   SUITES: "celery kombu" (default), or one of them
#   BACKEND_QUERY (env): appended to the result backend URL, e.g. "?protocol=2"
set -u
LABEL=$1; IMAGE=$2; SUITES=${3:-celery kombu}; PORT=7479
BQ=${BACKEND_QUERY:-}
start() {
  docker rm -f cel-$LABEL >/dev/null 2>&1
  case $IMAGE in
    redis:*) docker run -d --name cel-$LABEL --network host $IMAGE --port $PORT --save "" --appendonly no >/dev/null ;;
    *)       docker run -d --name cel-$LABEL --network host $IMAGE --port $PORT --protected-mode no >/dev/null ;;
  esac
  sleep 2; redis-cli -p $PORT info server | grep -E "^redis_version|kividb_version"
}
OUT=~/fw/out
for s in $SUITES; do
  start
  case $s in
    celery)
      cd ~/fw/celery
      # tox's 3.12-integration-redis environment, without -x so every test reports
      echo "TEST_BACKEND=redis://127.0.0.1:$PORT/0$BQ"
      TEST_BROKER=redis://127.0.0.1:$PORT TEST_BACKEND="redis://127.0.0.1:$PORT/0$BQ" REDIS_HOST=127.0.0.1 REDIS_PORT=$PORT \
        WORKER_LOGLEVEL=INFO PYTHONUNBUFFERED=1 \
        timeout 3600 ~/fw/venv-celery/bin/pytest -svv t/integration --timeout=300 \
        --junitxml=$OUT/celery-$LABEL.xml > $OUT/celery-$LABEL.log 2>&1
      echo "celery exit=$?"; tail -1 $OUT/celery-$LABEL.log ;;
    kombu)
      cd ~/fw/kombu
      # tox's integration-redis environment, without -x
      REDIS_HOST=127.0.0.1 REDIS_6379_TCP=$PORT \
        timeout 1800 ~/fw/venv-kombu/bin/pytest -v -E redis t/integration -n auto --reruns 2 --reruns-delay 1 \
        --junitxml=$OUT/kombu-$LABEL.xml > $OUT/kombu-$LABEL.log 2>&1
      echo "kombu exit=$?"; tail -1 $OUT/kombu-$LABEL.log ;;
  esac
  docker rm -f cel-$LABEL >/dev/null
done
