#!/usr/bin/env bash
# drive35.sh (on cl-cc): the cluster re-test on 35a413c0, in order: 1a local #516 repro (12 runs), 2c orphan repro
# (3 kill + 1 with 300 ms delay), p1 create + prefill, 1b p6 sweeps (b: 12 rounds 0-500 ms; c: 8 rounds near the
# handover), 2a kill -9 of a master x2, 2b kill -9 of the ASM source, 3 redis-cli reshard 1200 slots, 4 end state.
# No 1.2.0 control this time (the earlier pass has it; the harness is unchanged).
cd ~
PY=$HOME/venv/bin/python
V=35a413c0
for d in 0 0 0.02 0.05 0.05 0.08 0.1 0.1 0.12 0.14 0.16 0.2; do
  echo "$V $(timeout 600 $PY h/repro_fo_asm.py /opt/kv/kividb --runs 1 --delay $d 2>&1 | tail -1)" >> repro_local.txt
done
for d in 0 0 0; do echo "$V $(timeout 600 $PY h/repro_orphan.py /opt/kv/kividb --runs 1 --delay $d 2>&1 | tail -1)" >> repro_local.txt; done
echo "$V $(timeout 600 $PY h/repro_orphan.py /opt/kv/kividb --runs 1 --delay 0.3 2>&1 | tail -1)" >> repro_local.txt
pkill -9 -x kividb; echo RFO-DONE >> repro_local.txt
bash h/p1_create.sh > p1.log 2>&1
TAG=b bash h/p6_fo_asm.sh > p6b.log 2>&1
TAG=c DELAYS="0.33 0.35 0.36 0.37 0.38 0.38 0.39 0.39" bash h/p6_fo_asm.sh > p6c.log 2>&1
TAG=a ONLY=f1 bash h/p3_failover.sh > p3a.log 2>&1
TAG=b ONLY=f1 bash h/p3_failover.sh > p3b.log 2>&1
ONLY=f5 bash h/p3_failover.sh > p3f5.log 2>&1
ROUNDS="1:1200" bash h/p2a_reshard.sh > p2a.log 2>&1
bash h/p4_end.sh > p4.log 2>&1
echo DRIVE35-DONE >> p4.log
