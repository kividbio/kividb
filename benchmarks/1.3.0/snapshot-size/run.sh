#!/usr/bin/env bash
# #879 numbers: every variant on every dataset, tmpfs; the issue dataset on disk too.
cd "$(dirname "$0")"
PY=${PY:-python3}
# Seed once: for ds in issue text6m rand6m coll; do $PY bench.py seed $ds; done
for ds in issue text6m rand6m coll; do
  for v in v122 master yes no; do
    RUNDIR=/dev/shm/runs $PY bench.py run $v $ds --load --sync
  done
  RUNDIR=/dev/shm/runs $PY bench.py redis $ds --load --sync
done
for v in yes-inline; do
  for ds in issue coll; do
    RUNDIR=/dev/shm/runs $PY bench.py run $v $ds --load --sync
  done
done
for v in v122 master yes no; do
  RUNDIR=$HOME/runs $PY bench.py run $v issue
done
RUNDIR=$HOME/runs $PY bench.py redis issue
echo BATCH-DONE
