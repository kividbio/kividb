#!/usr/bin/env bash
# on cg: after callgrind.sh, Redis 8.10.3 under the same measurement in the same image
set -u
curl -sSfL https://github.com/redis/redis/archive/refs/tags/8.10.3.tar.gz -o /tmp/r.tgz
until grep -q CALLGRIND-DONE ~/cg.log; do sleep 30; done
sudo docker run --rm --security-opt seccomp=unconfined -v $HOME/harness:/h:ro -v /tmp/r.tgz:/r.tgz:ro -v $HOME/out/callgrind:/root/out/callgrind \
  kividb-ci-valgrind bash -c "cd /tmp && tar xzf /r.tgz && cd redis-8.10.3 && make -j8 >/dev/null 2>&1 && \
  RS=/tmp/redis-8.10.3/src/redis-server bash /h/callgrind-redis.sh" | tee ~/out/callgrind/redis.txt
echo CG-REDIS-DONE
