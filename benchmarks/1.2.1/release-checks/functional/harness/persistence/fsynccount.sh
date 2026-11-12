#!/usr/bin/env bash
# fsynccount.sh <kividb-binary> <io-model> <outfile>
# Counts the syncs the server makes per acknowledged write, for appendfsync always and everysec,
# from 1 and from 16 clients (redis-benchmark SET, random keys, no pipelining).
# Method: bpftrace on the server's pid (tgid, so io_uring io-wq workers count too):
#   syscalls fdatasync / fsync, kprobe vfs_fsync_range (catches every file sync in the kernel,
#   whichever path issued it, IORING_OP_FSYNC included), and io_uring_submit_req by opcode.
# Also: flush requests completed on the data disk (/proc/diskstats field 16 after the name) and INFO commandstats.
BIN=$1; IO=$2; OUT=$3; P=7700; DEV=$(basename $(readlink -f /dev/disk/azure/scsi1/lun0))   # the P30 data disk (sda on this VM)
: > $OUT
run() { # mode clients n
  local mode=$1 c=$2 n=$3 d=/data/fsc-$IO-$1-$2
  rm -rf $d; mkdir -p $d; printf 'appendonly yes\nappendfsync %s\n' $mode > $d/kividb.conf
  (cd $d && exec $BIN --port $P --protected-mode no --io-model $IO --dir $d --configfile $d/kividb.conf > $d/server.log 2>&1) &
  local spid=$!
  for i in $(seq 100); do redis-cli -p $P PING 2>/dev/null | grep -q PONG && break; sleep 0.1; done
  redis-cli -p $P CONFIG GET appendfsync | tr '\n' ' ' >> $OUT; echo >> $OUT
  grep -i -m2 'io_uring\|tokio\|no effect' $d/server.log >> $OUT
  sudo bpftrace -q -e "tracepoint:syscalls:sys_enter_fdatasync /pid == $spid/ { @fdatasync = count(); }
    tracepoint:syscalls:sys_enter_fsync /pid == $spid/ { @fsync = count(); }
    kprobe:vfs_fsync_range /pid == $spid/ { @vfs_fsync_range = count(); }" > $d/bpf1.txt 2>&1 &
  local b1=$!
  sudo bpftrace -q -e "tracepoint:io_uring:io_uring_submit_req /pid == $spid/ { @uring_opcode[args->opcode] = count(); }" > $d/bpf2.txt 2>&1 &
  local b2=$!
  sleep 4
  local fl0=$(awk -v d=$DEV '$3==d {print $19}' /proc/diskstats) wr0=$(awk -v d=$DEV '$3==d {print $8}' /proc/diskstats)
  local t0=$(date +%s.%N)
  redis-benchmark -p $P -t set -n $n -c $c -r 10000000 -P 1 --csv > $d/bench.csv 2>&1
  local t1=$(date +%s.%N)
  sleep 1.5   # let everysec's timer sync once more, so that it is counted
  local fl1=$(awk -v d=$DEV '$3==d {print $19}' /proc/diskstats) wr1=$(awk -v d=$DEV '$3==d {print $8}' /proc/diskstats)
  local calls=$(redis-cli -p $P INFO commandstats | grep -o 'cmdstat_set:calls=[0-9]*' | cut -d= -f2)
  local abw=$(redis-cli -p $P INFO persistence | grep aof_acks_before_write | tr -d '\r')
  sudo kill -INT $b1 $b2; wait $b1 $b2 2>/dev/null
  kill -9 $spid; wait $spid 2>/dev/null
  local vfs=$(grep -o 'vfs_fsync_range: [0-9]*' $d/bpf1.txt | awk '{print $2}')
  local fds=$(grep -o '@fdatasync: [0-9]*' $d/bpf1.txt | awk '{print $2}')
  local fs=$(grep -o '@fsync: [0-9]*' $d/bpf1.txt | awk '{print $2}')
  local rps=$(tail -1 $d/bench.csv | cut -d, -f2 | tr -d '"')
  local lat=$(tail -1 $d/bench.csv | cut -d, -f3,5 | tr -d '"')
  echo "RESULT io=$IO mode=$mode clients=$c sets=$calls rps=$rps lat_avg,p50_ms=$lat wall=$(echo "$t1 - $t0" | bc) vfs_fsync_range=${vfs:-0} fdatasync=${fds:-0} fsync=${fs:-0} uring_ops=[$(grep uring_opcode $d/bpf2.txt | tr -d ' ' | tr '\n' ' ')] bpf2err=[$(grep -i -m1 error $d/bpf2.txt)] disk_flushes=$((fl1-fl0)) disk_writes=$((wr1-wr0)) syncs_per_write=$(echo "scale=4; ${vfs:-0}/$calls" | bc) writes_per_sync=$(echo "scale=2; $calls/${vfs:-1}" | bc) $abw" | tee -a $OUT
}
for mode in always everysec; do
  if [ $mode = always ]; then n1=5000; n16=40000; else n1=50000; n16=300000; fi
  run $mode 1 $n1
  run $mode 16 $n16
done
