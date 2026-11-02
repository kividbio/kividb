#!/bin/bash
# build-all.sh   (runs on the builder VM under nohup) -- every engine in the order the queues need
# them, then waits for the v1.2.0-rc4 tag and builds it.
until grep -q SETUP-DONE ~/setup.log; do sleep 10; done
bash ~/build.sh redis 8.10.3 redis-8.10.3 > ~/build-redis-8.10.3.log 2>&1
bash ~/build.sh kividb-tar v1.1.3 kividb-1.1.3 > ~/build-kividb-1.1.3.log 2>&1
bash ~/build.sh redis 6bf6224c3dad518329ddc893ef9c5d58dcbabdeb redis-6bf6224 > ~/build-redis-6bf6224.log 2>&1
bash ~/build.sh kividb 48280b7a11f6aac236473272d1f49f6539640cd4 kividb-48280b7 > ~/build-kividb-48280b7.log 2>&1
bash ~/build.sh kividb origin/master kividb-master > ~/build-kividb-master.log 2>&1
until git ls-remote --tags https://github.com/kividbio/kividb v1.2.0-rc4 | grep -q rc4; do sleep 60; done
bash ~/build.sh kividb v1.2.0-rc4 kividb-rc4 > ~/build-kividb-rc4.log 2>&1
echo ALL-BUILT
