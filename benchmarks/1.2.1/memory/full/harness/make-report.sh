#!/bin/bash
# Rebuild results/ and charts/ from the raw copies of each client's /var/tmp/mem (raw/a: pair A, raw/b: pair B)
# and pair A's /var/tmp/wf (raw/wf).
set -euo pipefail
PY=${PY:-python3}
cd "$(dirname "$0")/.."
rm -rf charts results/raw results/tables*.md results/warefeats; mkdir -p charts results/warefeats
$PY -I harness/analyze.py raw/a raw/b raw/wf charts > results/tables.md
for d in a b; do $PY -I harness/compact.py raw/$d results/raw/pair-$d; done
rm -f results/raw/pair-*/dels-*.resp
cp raw/wf/*.json results/warefeats/
du -sh results charts
