#!/usr/bin/env bash
# powerloss_rounds.sh <build> <io> <mode> <round> : one power-loss-like round, run from the operator's machine.
# The server VM (pvm) is rebooted with sysrq-b (no sync, page cache lost: like a power cut for everything that was
# not on the disk); the client VM (pcli) keeps the acks. Disk: premium SSD, host caching None (write-through).
K=${KEY:?}; SV=${SV:?}; CL=${CL:?}; PRIV=10.0.0.4; PORT=7800
O="-i $K -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ConnectTimeout=15 -o BatchMode=yes -o ServerAliveInterval=5 -o ServerAliveCountMax=2"
b=$1; io=$2; mode=$3; r=$4; d=/data/pl-$b-$io-$mode-$r; j=pl-$b-$io-$mode-$r.json
start_srv='cd '$d' && (nohup ~/bin/kividb-'$b' --port '$PORT' --protected-mode no --io-model '$io' --dir '$d' --configfile '$d'/kividb.conf >> '$d'/server.log 2>&1 < /dev/null &); for i in $(seq 600); do redis-cli -p '$PORT' PING 2>/dev/null | grep -q PONG && break; sleep 0.2; done; redis-cli -p '$PORT' CONFIG GET appendfsync | tr "\n" " "; grep -m1 "I/O model" '$d'/server.log'
ssh $O azureuser@$SV "rm -rf $d; mkdir -p $d; printf 'appendonly yes\nappendfsync $mode\n' > $d/kividb.conf; sync; $start_srv; uptime -s"
ssh $O azureuser@$CL "rm -f $j; nohup ~/venv/bin/python -I ~/powerloss.py write $PRIV $PORT $j > $j.log 2>&1 < /dev/null &"
sleep $((4 + RANDOM % 5))
timeout 20 ssh $O azureuser@$SV 'sudo sh -c "echo 1 > /proc/sys/kernel/sysrq; echo b > /proc/sysrq-trigger"' ; echo "sysrq-b sent ($?)"
for i in $(seq 30); do ssh $O azureuser@$CL "cat $j.log" 2>/dev/null | grep -q acked && break; sleep 2; done
ssh $O azureuser@$CL "cat $j.log"
for i in $(seq 40); do ssh $O azureuser@$SV 'mountpoint -q /data && uptime -s' 2>/dev/null && break; sleep 15; done
ssh $O azureuser@$SV "$start_srv"
ssh $O azureuser@$CL "~/venv/bin/python -I ~/powerloss.py verify $PRIV $PORT $j"
ssh $O azureuser@$SV "redis-cli -p $PORT INFO persistence | grep -E 'aof_lost_records|aof_acks_before_write' | tr -d '\r' | tr '\n' ' '; pkill -9 -f '^/home/azureuser/bin/kividb'; ls -la $d | tail -n +2 | awk '{print \$5, \$9}' | tr '\n' ' '"
