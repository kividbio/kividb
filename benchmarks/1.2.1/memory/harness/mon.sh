#!/bin/bash
# Server-side: start/stop the 1-second memory sampler for the running engine.
#   mon.sh start <run-name>    writes /var/tmp/mem/<run-name>.csv
#   mon.sh stop
set -euo pipefail
D=$(dirname "$0")
mkdir -p /var/tmp/mem
case $1 in
  start)
    rm -f /var/tmp/mem/sampler.stop
    CG=$("$D/eng.sh" cgroup); PID=$("$D/eng.sh" pid)
    sudo nohup python3 "$D/sampler.py" "/var/tmp/mem/$2.csv" "$CG" "$PID" 1 > "/var/tmp/mem/$2.sampler.log" 2>&1 < /dev/null &
    sleep 1; echo "sampler started for $2 (pid $PID, $CG)"
    ;;
  stop)
    touch /var/tmp/mem/sampler.stop; sleep 2
    sudo pkill -f "[s]ampler.py" || true
    ;;
esac
