#!/usr/bin/env bash
# Runs on the SERVER. One engine resident at a time, started and stopped by
# the PID recorded at start (vector-benchmark.yml's vb_start/vb_stop), plus a
# 1-second sampler of the engine's memory while it runs.
#
#   server_ctl.sh start  <label>          start the engine, wait for its port
#   server_ctl.sh stop   <label>          print peak RSS, stop it, wait for the port to close
#   server_ctl.sh sample <label> <csv>    sampler in the background (stops with the engine)
#
# Labels: kividb-<name> (binary ~/bins/<name>/kividb), redis, redis-mt, dragonfly.
# Engine flags:
#   kividb    --bind 0.0.0.0 --port 6380 --loglevel error            (workflow; default threads = cores)
#   redis     --port 6381 --save '' --appendonly no --loadmodule redisearch.so   (workflow)
#   redis-mt  the same + --io-threads 8 (Redis's I/O-thread knob for a multi-core box;
#             the query engine's search-workers is already 16 by default in 8.10.2,
#             in both configurations; the start prints both values)
#   dragonfly --port 6382 --proactor_threads=$(nproc) --dbfilename=   (workflow)
set -uo pipefail
CMD=$1 LABEL=$2
port_of() { case $1 in kividb-*) echo 6380;; redis|redis-mt) echo 6381;; dragonfly) echo 6382;; esac; }
PORT=$(port_of "$LABEL")
PIDF=/tmp/engine-$LABEL.pid
ulimit -n 65535 || true

case $CMD in
start)
  for p in 6380 6381 6382; do
    if (exec 3<>/dev/tcp/127.0.0.1/$p) 2>/dev/null; then echo "busy :$p before $LABEL" >&2; exit 1; fi
  done
  case $LABEL in
    kividb-*)
      BIN=~/bins/${LABEL#kividb-}/kividb
      mkdir -p /tmp/kdir && cd /tmp/kdir && rm -f /tmp/kdir/*
      nohup "$BIN" --bind 0.0.0.0 --port 6380 --loglevel error > /tmp/$LABEL.log 2>&1 & ;;
    redis)
      cd /tmp
      nohup redis-server --port 6381 --bind 0.0.0.0 --protected-mode no --save '' --appendonly no \
        --loadmodule "$(cat ~/redisearch-path)" > /tmp/$LABEL.log 2>&1 & ;;
    redis-mt)
      cd /tmp
      nohup redis-server --port 6381 --bind 0.0.0.0 --protected-mode no --save '' --appendonly no \
        --io-threads 8 --loadmodule "$(cat ~/redisearch-path)" > /tmp/$LABEL.log 2>&1 & ;;
    dragonfly)
      mkdir -p /tmp/dfdir && cd /tmp/dfdir
      nohup ~/dragonfly-aarch64 --port 6382 --bind 0.0.0.0 --proactor_threads="$(nproc)" \
        --dbfilename= --logtostderr > /tmp/$LABEL.log 2>&1 & ;;
    *) echo "unknown label $LABEL" >&2; exit 1 ;;
  esac
  echo $! > $PIDF
  for _ in $(seq 1 30); do
    if (exec 3<>/dev/tcp/127.0.0.1/$PORT) 2>/dev/null; then
      case $LABEL in redis*)
        echo "$LABEL: $(redis-cli -p 6381 CONFIG GET io-threads | tr '\n' ' ') $(redis-cli -p 6381 CONFIG GET search-workers | tr '\n' ' ')" ;;
      esac
      exit 0
    fi
    sleep 1
  done
  echo "$LABEL never came up" >&2; tail -20 /tmp/$LABEL.log >&2; exit 1 ;;
sample)
  OUT=$3
  PID=$(cat $PIDF)
  nohup bash -c '
    PID='"$PID"' PORT='"$PORT"' OUT='"$OUT"'
    echo "t,rss_kb,used_memory,dbsize,cpu_ticks" > "$OUT"
    while kill -0 $PID 2>/dev/null; do
      t=$(date +%s.%N)
      rss=$(awk "/^VmRSS/ {print \$2}" /proc/$PID/status 2>/dev/null)
      ticks=$(awk "{print \$14+\$15}" /proc/$PID/stat 2>/dev/null)
      um=$(timeout 2 redis-cli -p $PORT INFO memory 2>/dev/null | tr -d "\r" | awk -F: "/^used_memory:/ {print \$2}")
      ks=$(timeout 2 redis-cli -p $PORT DBSIZE 2>/dev/null | tr -d "\r")
      echo "${t:0:14},$rss,$um,$ks,$ticks" >> "$OUT"
      sleep 0.5
    done' > /dev/null 2>&1 < /dev/null &
  echo $! > /tmp/sampler-$LABEL.pid ;;
stop)
  # Optional linger (seconds, from ~/linger): keep the engine, and so the
  # sampler, running after the tool has deleted its data, to see how much
  # memory each engine gives back to the OS and how fast.
  sleep "$(cat ~/linger 2>/dev/null || echo 0)"
  PID=$(cat $PIDF 2>/dev/null || true)
  if [ -n "$PID" ]; then
    echo "$LABEL peak_rss_kb=$(awk '/VmHWM/ {print $2}' /proc/$PID/status 2>/dev/null)"
    kill "$PID" 2>/dev/null || true
    for _ in $(seq 1 60); do kill -0 "$PID" 2>/dev/null || break; sleep 1; done
    kill -9 "$PID" 2>/dev/null || true
  fi
  for _ in $(seq 1 30); do (exec 3<>/dev/tcp/127.0.0.1/$PORT) 2>/dev/null || break; sleep 1; done
  rm -rf /tmp/kdir/* /tmp/dfdir/* 2>/dev/null
  sleep 5 ;;
esac
