#!/bin/bash
# queue.sh   (runs on a client VM under nohup)
# Works through ~/jobs.txt, one line per run: "<tag> <engine> <policy> <benchtime>".
# For each: restart the engine on the paired server (~/server.txt), run the reporter's client
# command, collect the server's numbers, store everything in ~/results/. A job whose binary
# has not reached the server yet is skipped until it has. Lines may be appended while it runs;
# touch ~/STOP to make it exit after the current run.
SSHO="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ConnectTimeout=15"
S=$(cat ~/server.txt)
mkdir -p ~/results; touch ~/done.txt
while [ ! -e ~/STOP ]; do
  job=""
  while read -r tag engine policy n; do
    [ -z "$tag" ] && continue
    grep -qx "$tag" ~/done.txt && continue
    ssh -n $SSHO "$S" "test -f bin/$engine.info" || continue
    job="$tag $engine $policy $n"; break
  done < ~/jobs.txt
  if [ -z "$job" ]; then sleep 30; continue; fi
  set -- $job; tag=$1; engine=$2; policy=$3; n=$4
  echo "$(date -u +%FT%T) start $job"
  ssh -n $SSHO "$S" "bash ~/server-run.sh $engine $policy" > ~/results/start-$tag.txt 2>&1
  ping -c 20 -i 0.2 -q "$S" > ~/results/ping-$tag.txt 2>&1
  mpstat -P ALL 30 > ~/results/client-mpstat-$tag.txt 2>&1 &
  MP=$!
  {
    echo "cmd: taskset -c 1,2,3 ./bench.test -test.run=^\$ -test.bench=^BenchmarkRedis\$ -test.benchtime=${n}x -test.timeout=0 true 1073741824 1024 8 1 0 $S"
    echo "pair: $(hostname) -> $S"
    date -u
    cd ~/bench
    /usr/bin/time -v timeout 4h taskset -c 1,2,3 ~/bench.test -test.run='^$' -test.bench='^BenchmarkRedis$' \
      -test.benchtime="${n}x" -test.timeout=0 true 1073741824 1024 8 1 0 "$S"
    echo "exit=$?"
    date -u
  } > ~/results/bench-$tag.log 2>&1
  kill $MP
  ssh -n $SSHO "$S" "bash ~/server-collect.sh $tag" >/dev/null 2>&1
  scp $SSHO "$S:run/final-$tag.txt" "$S:run/samples-$tag.log" ~/results/ >/dev/null 2>&1
  echo "$tag" >> ~/done.txt
  echo "$(date -u +%FT%T) done $tag"
done
echo "stopped"
