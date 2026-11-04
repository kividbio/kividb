#!/usr/bin/env bash
# bld8.sh (on bld, after build8.sh): the single-host checks of the rc8 failover subset. Output in ~/out.
#  3. expire-replay repro (expirefix): expire_replay.sh, field_replay.sh, expire_more.sh, 2 runs each
#  4. #303: two engines in one data directory, under write load with the AOF on (r303load.sh)
#  5. MIGRATE: the repo's --io-model tokio probe, the slow-destination probe, and MIGRATE stopped by TRYAGAIN
#     (migtry.sh: the destination must not keep the copies; a second MIGRATE without REPLACE must succeed)
set -u
ulimit -n 65535
H=~/fo/harness PY=~/venv/bin/python B=/opt/kv/kividb
O=~/out; mkdir -p $O/expire $O/r303 $O/migrate
S=$O/summary.txt; : > $S
note() { echo "$(date -u +%T) $*" | tee -a $S; }
note "binary: $($B --version | head -1); $(sha256sum $B)"
# ---------- 3. expire replay ----------
for rep in 1 2; do
  for t in expire_replay field_replay expire_more; do bash $H/$t.sh $B rc8 > $O/expire/$t-rc8-$rep.txt 2>&1; done
done
for t in expire_replay field_replay expire_more; do note "== $t"; grep -h -E 'after restart|deletion records' $O/expire/$t-*.txt | sort | uniq -c | tee -a $S; done
note EXPIRE-DONE
# ---------- 4. #303 ----------
for rep in 1 2; do
  for d in 0 200 600; do bash $H/r303load.sh stop $d $rep $O/r303 2>&1 | tail -1 | tee -a $S; done
  bash $H/r303load.sh never 0 $rep $O/r303 2>&1 | tail -1 | tee -a $S
done
note R303-DONE
# ---------- 5. MIGRATE ----------
cd /tmp
for rep in 1 2; do
  timeout 600 $PY ~/b/fixes/migrate-tokio-stall/probe.py $B 10 0.2 > $O/migrate/probe-delay0.2-$rep.txt 2>&1
  note "probe delay0.2 rep$rep: $(tail -1 $O/migrate/probe-delay0.2-$rep.txt)"
  timeout 600 $PY ~/b/fixes/migrate-tokio-stall/probe.py $B 10 0 > $O/migrate/probe-delay0-$rep.txt 2>&1
  note "probe delay0 rep$rep: $(tail -1 $O/migrate/probe-delay0-$rep.txt)"
  timeout 600 $PY $H/migrate_slowdst.py $B 10 0.2 3 5000 > $O/migrate/slowdst-$rep.txt 2>&1
  note "slowdst rep$rep: $(tail -1 $O/migrate/slowdst-$rep.txt)"
  timeout 600 bash $H/migtry.sh $B > $O/migrate/tryagain-$rep.txt 2>&1
  note "tryagain rep$rep:"; tee -a $S < $O/migrate/tryagain-$rep.txt
done
note MIGRATE-DONE
note ALL-DONE
