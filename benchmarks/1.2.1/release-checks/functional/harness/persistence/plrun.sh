#!/usr/bin/env bash
# plrun.sh (laptop): wait (bounded, max 90 min) for drive.sh on fp to finish, then the 10 power-loss rounds of the
# earlier pass (always: default uring x3, default tokio x2, full uring, full tokio; everysec: default uring x2,
# default tokio x1). Logs to ../results/persistence/powerloss/.
cd "$(dirname "$0")"
export KEY=<local path>
export SV=<vm ip> CL=<vm ip>
O=../../results/persistence/powerloss; mkdir -p $O
for i in $(seq 1 540); do bash ../ssh.sh $SV 'grep -q DRIVE-DONE drive.log' && break; sleep 10; done
bash ../ssh.sh $SV 'grep -q DRIVE-DONE drive.log' || { echo "drive.sh not done after 90 min" > $O/ABORTED; exit 1; }
for r in "default uring always 1" "default uring always 2" "default uring always 3" "default tokio always 1" \
         "default tokio always 2" "full uring always 1" "full tokio always 1" "default uring everysec 1" \
         "default uring everysec 2" "default tokio everysec 1"; do
  set -- $r
  timeout 1200 bash powerloss_rounds.sh $1 $2 $3 $4 > $O/$1-$2-$3-$4.log 2>&1 < /dev/null
done
echo PL-DONE > $O/PL-DONE
