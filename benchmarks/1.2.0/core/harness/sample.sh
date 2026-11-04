#!/usr/bin/env bash
# Machine-wide CPU and NIC counters over a window, as one CSV line:
#   sample.sh <skip-seconds> <window-seconds>
# -> busy_pct,user_pct,sys_pct,softirq_pct,rx_MBps,tx_MBps,ncpu
# Run on the server and on each load generator during a scenario. CPU time is
# the share of all vCPUs; user vs sys+softirq separates command work from the
# kernel's network and syscall work.
set -euo pipefail
sleep "$1"
W=$2
dev=$(ip -o route get 10.40.0.1 2>/dev/null | sed -n 's/.* dev \([^ ]*\).*/\1/p'); dev=${dev:-eth0}
read_cpu() { awk '/^cpu /{print $2+$3, $4, $5+$6, $7, $8}' /proc/stat; }   # user+nice sys idle+iowait irq softirq
read_net() { awk -v d="$dev:" '$1==d{print $2, $10}' /proc/net/dev; }
read -r u1 s1 i1 q1 sq1 < <(read_cpu); read -r r1 t1 < <(read_net)
sleep "$W"
read -r u2 s2 i2 q2 sq2 < <(read_cpu); read -r r2 t2 < <(read_net)
awk -v u=$((u2-u1)) -v s=$((s2-s1)) -v i=$((i2-i1)) -v q=$((q2-q1)) -v sq=$((sq2-sq1)) \
    -v r=$((r2-r1)) -v t=$((t2-t1)) -v w="$W" -v n="$(nproc)" 'BEGIN{
  tot=u+s+i+q+sq; if (tot==0) tot=1
  printf "%.1f,%.1f,%.1f,%.1f,%.1f,%.1f,%d\n", 100*(u+s+q+sq)/tot, 100*u/tot, 100*s/tot, 100*(q+sq)/tot, r/w/1e6, t/w/1e6, n }'
