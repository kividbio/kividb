#!/bin/bash
# Server-side: one cold start of <reader> on the dump written by <writer>, timed.
#   loadone.sh <reader> <writer> <label>
# The dump is /mnt/nvme/dumps/<writer>.kdb (written by loadtime.sh), hard-linked in as /mnt/nvme/data/dump.kdb,
# page cache dropped, then the engine is started the way eng.sh starts it (profile "plain": no AOF, so the
# snapshot is what it loads). KiviDB loads the snapshot before it listens, so the first PONG marks the end of
# the load. Every 0.1 s: PING, and the process's VmRSS (max kept). After the first PONG: DBSIZE, INFO memory,
# VmHWM (the process's peak RSS over the whole load), and RSS again 10 s later.
# Prints one CSV line: label,reader,writer,start_to_pong_s,dbsize,used_memory,rss_at_pong_kb,vmhwm_kb,max_sampled_rss_kb,rss_10s_kb,used_10s,log_load_s
set -uo pipefail
R=$1; W=$2; LBL=$3
NAME=memeng; DATA=/mnt/nvme/data; DUMP=/mnt/nvme/dumps/$W.kdb
IMG=$(cat /var/tmp/$R-image)
sudo docker rm -f $NAME > /dev/null 2>&1 || true
sudo rm -rf ${DATA:?}/*; sudo mkdir -p $DATA && sudo chmod 777 $DATA
sudo ln "$DUMP" $DATA/dump.kdb
SZ0=$(stat -c '%s %Y' "$DUMP")
sync; echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null; sleep 2
T0=$(date +%s.%N)
sudo docker run -d --name $NAME --network host --security-opt seccomp=unconfined --ulimit memlock=-1 \
  --ulimit nofile=200000:200000 -v $DATA:/data "$IMG" --port 6379 --bind 0.0.0.0 --protected-mode no --dir /data > /dev/null
PID=$(sudo docker inspect -f '{{.State.Pid}}' $NAME)
MAX=0; T1=""
for _ in $(seq 1 6000); do
  r=$(awk '/^VmRSS:/{print $2}' /proc/$PID/status 2>/dev/null || echo 0); [ "${r:-0}" -gt "$MAX" ] && MAX=$r
  if redis-cli -p 6379 ping 2>/dev/null | grep -q PONG; then T1=$(date +%s.%N); break; fi
  if ! sudo docker inspect -f '{{.State.Running}}' $NAME 2>/dev/null | grep -q true; then break; fi
  sleep 0.1
done
if [ -z "$T1" ]; then
  sudo docker logs $NAME > /var/tmp/mem/loadone-$LBL-$R-$W.log 2>&1
  echo "$LBL,$R,$W,FAIL,,,,,$MAX,,,"; sudo docker rm -f $NAME > /dev/null 2>&1; exit 0
fi
DB=$(redis-cli -p 6379 dbsize)
USED=$(redis-cli -p 6379 info memory | tr -d '\r' | awk -F: '/^used_memory:/{print $2}')
RSSP=$(awk '/^VmRSS:/{print $2}' /proc/$PID/status); HWM=$(awk '/^VmHWM:/{print $2}' /proc/$PID/status)
sleep 10
RSS10=$(awk '/^VmRSS:/{print $2}' /proc/$PID/status)
USED10=$(redis-cli -p 6379 info memory | tr -d '\r' | awk -F: '/^used_memory:/{print $2}')
sudo docker logs $NAME > /var/tmp/mem/loadone-$LBL-$R-$W.log 2>&1
# CHECK=<keys>: a content fingerprint of what was loaded (contentcheck.sh)
[ -n "${CHECK:-}" ] && bash "$(dirname "$0")/contentcheck.sh" "$CHECK" > /var/tmp/mem/check-$LBL-$R-$W.txt 2>&1
# load time from the engine's own log lines ("Found dump.kdb" .. "Loaded N keys"), when they carry times
LOGS=$(python3 - /var/tmp/mem/loadone-$LBL-$R-$W.log <<'PY'
import re, sys, datetime
a = b = None
for line in open(sys.argv[1], errors="replace"):
    m = re.search(r"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?)", line) or re.search(r"(\d\d \w{3} \d{4} \d\d:\d\d:\d\d\.\d+)", line)
    if not m: continue
    s = m.group(1)
    try:
        t = datetime.datetime.fromisoformat(s) if "T" in s else datetime.datetime.strptime(s, "%d %b %Y %H:%M:%S.%f")
    except ValueError:
        continue
    if "Found dump.kdb" in line: a = t
    if "Loaded" in line and "from snapshot" in line: b = t
print(f"{(b - a).total_seconds():.3f}" if a and b else "")
PY
)
sudo docker rm -f $NAME > /dev/null 2>&1
SZ1=$(stat -c '%s %Y' "$DUMP"); [ "$SZ0" = "$SZ1" ] || echo "WARNING dump changed: $SZ0 -> $SZ1" >&2
echo "$LBL,$R,$W,$(awk -v a="$T0" -v b="$T1" 'BEGIN{printf "%.3f", b - a}'),$DB,$USED,$RSSP,$HWM,$MAX,$RSS10,$USED10,$LOGS"
