#!/bin/bash
# As run on the test VM: clones and venvs under ~/fw (see the setup notes in the docs page).
# Spring AI 2.0.2's own Redis integration tests (vector store, semantic cache, chat memory repository).
# They start `redis/redis-stack-server:latest` and `redis/redis-stack:latest` with Testcontainers; here
# both names are local tags of the server under test, listening on 6379 as Testcontainers expects.
# usage: spring_upstream.sh LABEL IMAGE
set -u
LABEL=$1; IMAGE=$2
OUT=~/fw/out/spring-$LABEL; rm -rf $OUT; mkdir -p $OUT
B=/tmp/subst-$LABEL; mkdir -p $B
case $IMAGE in
  redis:*) printf 'FROM %s\n' "$IMAGE" > $B/Dockerfile ;;
  *) printf 'FROM %s\nCMD ["--port","6379","--protected-mode","no"]\n' "$IMAGE" > $B/Dockerfile ;;
esac
docker build -q -t subst-$LABEL $B
docker tag subst-$LABEL redis/redis-stack-server:latest
docker tag subst-$LABEL redis/redis-stack:latest
docker run --rm redis/redis-stack-server:latest --version 2>/dev/null | head -1 || true
cd ~/fw/spring-ai
MODS=vector-stores/spring-ai-redis-store,vector-stores/spring-ai-redis-semantic-cache,memory-repositories/spring-ai-model-chat-memory-repository-redis
timeout 3600 ./mvnw -B -Pintegration-tests -pl $MODS verify -Dcheckstyle.skip=true -Dspring-javaformat.skip=true \
  -Dmaven.javadoc.skip=true -Dmaven.build.cache.enabled=false -Dmaven.test.failure.ignore=true -Dfailsafe.failIfNoSpecifiedTests=false > $OUT/mvn.log 2>&1
echo "mvn exit=$?"
for m in ${MODS//,/ }; do cp $m/target/failsafe-reports/TEST-*.xml $m/target/surefire-reports/TEST-*.xml $OUT/ 2>/dev/null; done
grep -E "Tests run:.*Fail" $OUT/mvn.log | tail -8
