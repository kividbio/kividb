#!/usr/bin/env bash
# chain.sh: after p6b. p3 f1 twice (kill -9 of a master), f5 (kill -9 of the ASM source), p2a one redis-cli reshard,
# p4 end state on 878746e4; then the control: all six nodes on the 1.2.0 release binary, p6 again (TAG=c120).
cd ~
for i in $(seq 1 200); do grep -q P6-DONE p6b.log && break; sleep 10; done
TAG=a ONLY=f1 bash h/p3_failover.sh > p3a.log 2>&1
TAG=b ONLY=f1 bash h/p3_failover.sh > p3b.log 2>&1
ONLY=f5 bash h/p3_failover.sh > p3f5.log 2>&1
ROUNDS="1:1200" bash h/p2a_reshard.sh > p2a.log 2>&1
bash h/p4_end.sh > p4.log 2>&1
echo CHAIN-878-DONE >> p4.log
