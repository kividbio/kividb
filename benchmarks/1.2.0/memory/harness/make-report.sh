#!/bin/bash
# Rebuild results/ and charts/ from the raw copies of each client's /var/tmp/mem (raw/a, raw/b: first runs;
# raw/a2, raw/b2: the snapshot reruns with distinct client seeds) and /var/tmp/wf (raw/wf).
set -euo pipefail
PY=${PY:-python3}
cd "$(dirname "$0")/.."
rm -rf charts results/raw results/tables*.md; mkdir -p charts results/warefeats
E=$(mktemp -d)
TAG=-hotset TITLE_SUFFIX=" (hot-set writes)" $PY -I harness/analyze.py raw/a raw/wf charts > results/tables.md
rm -f charts/mixed-*-hotset.svg
TAG=-pair-b TITLE_SUFFIX=" (pair B, E32ds_v6)" $PY -I harness/analyze.py raw/b "$E" charts > results/tables-pair-b.md
TAG=-uniform TITLE_SUFFIX=" (uniform random writes)" $PY -I harness/analyze.py raw/a2 "$E" charts > results/tables-uniform.md
TAG=-uniform-pair-b TITLE_SUFFIX=" (uniform random writes, pair B)" $PY -I harness/analyze.py raw/b2 "$E" charts > results/tables-uniform-pair-b.md
for d in a b a2 b2; do $PY -I harness/compact.py raw/$d results/raw/pair-$d; done
rm -f results/raw/pair-a/mixed-dragonfly* results/raw/pair-b/mixed-redis*
cp raw/wf/*.json results/warefeats/
du -sh results charts
