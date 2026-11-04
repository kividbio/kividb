#!/usr/bin/env bash
# bld.sh: runs on the build VM under nohup. Output in ~/out.
#  1. expire-replay repro on the published rc6 and rc7 binaries (and Redis 8.10.3 as the reference)
#  2. MIGRATE probe (--io-model tokio) on rc7, with rc4 and rc6 as controls
#  3. Parikalp's slab race test: v1.2.0-rc7 + port-a.patch, 20 runs (+3 run directly as the child);
#     control: v1.2.0-rc4 + his r12-repro-on-a6b1bf3.patch, 10 runs
# Needs: /opt/kv (rc7), /opt/kvrc6, /opt/kvrc4 (published binaries), /opt/redis/redis-server, ~/b (harnesses),
#        ~/p/{port-a.patch,r12-repro-on-a6b1bf3.patch}, ~/h/{expire_replay.sh,field_replay.sh,expire_more.sh}
set -u
ulimit -n 65535
O=~/out; mkdir -p $O/expire $O/migrate $O/slab
S=$O/summary.txt; : > $S
note() { echo "$(date -u +%T) $*" | tee -a $S; }

# ---------- 1. expire replay ----------
for rep in 1 2 3; do
  for b in rc6:/opt/kvrc6/kividb rc7:/opt/kv/kividb redis:/opt/redis/redis-server; do
    l=${b%%:*}; bin=${b#*:}
    for t in expire_replay field_replay expire_more; do
      bash ~/h/$t.sh "$bin" "$l" > $O/expire/$t-$l-$rep.txt 2>&1
    done
  done
done
for t in expire_replay field_replay expire_more; do
  note "== $t"; grep -h -E 'after restart' $O/expire/$t-*-*.txt | sort | uniq -c | tee -a $S
done
note EXPIRE-DONE

# ---------- 2. MIGRATE probe ----------
PY=~/venv/bin/python
cd /tmp
for b in rc7:/opt/kv/kividb rc6:/opt/kvrc6/kividb rc4:/opt/kvrc4/kividb; do
  l=${b%%:*}; bin=${b#*:}
  timeout 600 $PY ~/b/fixes/migrate-tokio-stall/probe.py $bin 10 0.2 > $O/migrate/$l-delay0.2.txt 2>&1
  note "probe $l delay0.2: $(tail -1 $O/migrate/$l-delay0.2.txt)"
done
timeout 600 $PY ~/b/fixes/migrate-tokio-stall/probe.py /opt/kv/kividb 10 0 > $O/migrate/rc7-delay0.txt 2>&1
note "probe rc7 delay0: $(tail -1 $O/migrate/rc7-delay0.txt)"
# a longer wait on the destination: MIGRATE held 20 s at the failpoint is covered by the probe's own loop (PING 3 s)
note MIGRATE-DONE

# ---------- 3. slab race test ----------
. ~/.cargo/env
export CARGO_TARGET_DIR=~/target
T1=value_slab::tests::plan_evacuation_never_takes_a_slab_a_thread_owns
runs() { # n name dir
  local n=$1 name=$2 dir=$3 pass=0 fail=0 i
  cd $dir
  for i in $(seq 1 $n); do
    if timeout 300 cargo test --release --lib -- --exact "$T1" --nocapture > $O/slab/$name-$i.log 2>&1 < /dev/null; then
      pass=$((pass+1)); else fail=$((fail+1)); fi
  done
  note "$name: $pass passed, $fail failed of $n"
  grep -h "panicked at\|^  left\|^ right\|test result" $O/slab/$name-*.log | sed 's/finished in [0-9.]*s/finished/' | sort | uniq -c | sort -rn | head -8 | tee -a $S
}
cd ~/kividb
git -C ~/kividb worktree add -q ~/rc7 v1.2.0-rc7 2>/dev/null
git -C ~/kividb worktree add -q ~/rc4 v1.2.0-rc4 2>/dev/null
cd ~/rc7 && note "rc7 tree: $(git rev-parse --short HEAD) $(git describe --tags)"
files=$(git apply --numstat ~/p/port-a.patch | awk '{print $3}' | sort -u); note "port-a.patch touches: $files"
git apply ~/p/port-a.patch && note "port-a.patch applied on rc7" || note "port-a.patch DID NOT APPLY"
git diff --stat | tee -a $S
cargo test --release --lib --no-run > $O/slab/rc7-build.log 2>&1 < /dev/null || { note "rc7 build FAILED"; tail -30 $O/slab/rc7-build.log >> $S; }
runs 20 rc7 ~/rc7
KIVIDB_VALUE_SLAB_ALONE=1 runs 3 rc7-child-direct ~/rc7
# the whole value_slab module, with the new test in it
cd ~/rc7; timeout 1800 cargo test --release --lib value_slab > $O/slab/rc7-value_slab-module.log 2>&1 < /dev/null; note "rc7 lib value_slab module: $(grep '^test result' $O/slab/rc7-value_slab-module.log)"
cd ~/rc4 && note "rc4 tree: $(git rev-parse --short HEAD)"
git apply ~/p/r12-repro-on-a6b1bf3.patch && note "r12 repro applied on rc4" || note "r12 repro DID NOT APPLY on rc4"
cargo test --release --lib --no-run > $O/slab/rc4-build.log 2>&1 < /dev/null || { note "rc4 build FAILED"; tail -30 $O/slab/rc4-build.log >> $S; }
runs 10 rc4 ~/rc4
note SLAB-DONE
note ALL-DONE
