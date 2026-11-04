#!/usr/bin/env bash
# on srv: the whole rc7 session, in order (one engine on the server at a time).
set -u
cd ~
log() { echo "$(date -u +%T) $*" | tee -a ~/master.log; }
until [ -x ~/bin/kividb-cand ] && [ -x ~/bin/kividb-rc4 ]; do sleep 20; done
log "binaries: $(~/bin/kividb-cand --version) / $(~/bin/kividb-rc4 --version) / $(~/bin/kividb-1.1.3 --version)"
# 1. the matrix: KiviDB builds in three alternating rounds, competitors in between
bash harness/run-all.sh kividb-cand kividb-rc4 kividb-1.1.3 redis \
  kividb-rc4:kividb-rc4-r2 kividb-cand:kividb-cand-r2 valkey dragonfly \
  kividb-cand:kividb-cand-r3 kividb-rc4:kividb-rc4-r3 redis-cluster kvrocks > ~/run-all.log 2>&1
log "matrix done"
# 2. 2 KB sustained re-check
bash harness/sustained-2k.sh kividb-cand kividb-rc4 dragonfly redis valkey kividb-1.1.3 > ~/sustained.log 2>&1
log "sustained done"
# 3. the LPOP tail repro, both builds, alternating
for e in kividb-cand kividb-rc4 kividb-cand kividb-rc4; do
  echo "== $e" >> ~/lpop-tail.log
  DEFRAG=yes bash harness/lpop-tail.sh "$e" >> ~/lpop-tail.log 2>&1
done
log "lpop-tail done"
# 4. profiles: frame-pointer builds (from the client VM, built there with the same toolchain)
until ssh 10.0.0.5 'test -x bin/kividb-cand-fp && test -x bin/kividb-rc4-fp && test -x bin/kividb-1.1.3-fp'; do sleep 30; done
scp -q 10.0.0.5:bin/kividb-cand-fp 10.0.0.5:bin/kividb-rc4-fp 10.0.0.5:bin/kividb-1.1.3-fp ~/bin/
for e in kividb-cand-fp kividb-rc4-fp redis kividb-1.1.3-fp; do bash harness/profile.sh "$e" >> ~/profile.log 2>&1; done
# release binaries, three alternating rounds (CPU per request without frame pointers)
for r in 1 2 3; do
  if [ $r = 2 ]; then order="kividb-rc4 kividb-cand"; else order="kividb-cand kividb-rc4"; fi
  for e in $order; do RUN=$e-r$r bash harness/profile.sh "$e" >> ~/profile.log 2>&1; done
done
log "profiles done"
# 5. LPOP stall profile of the candidate (frame pointers)
bash harness/lpop-perf.sh kividb-cand-fp > ~/lpop-perf.log 2>&1
log "MASTER-DONE"
