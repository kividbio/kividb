#!/usr/bin/env bash
# merge.sh (on foc): set A's reps 1-2 + set B's rep 3 and kill9_checkpoint -> ~/merged; private IPs -> VM names;
# then the report (tables + charts) into ~/rep/out and a tarball of the runs without acked.json.
set -eu
rm -rf ~/merged /tmp/bres ~/rep/out; mkdir -p ~/merged /tmp/bres ~/rep/out
cp -r ~/fo-results/. ~/merged/
tar xzf ~/fo-b.tgz -C /tmp/bres
cp -r --update=none /tmp/bres/fo-results/. ~/merged/
find ~/merged -name acked.json -delete
# VM names instead of private addresses (text files only)
map='s/10\.60\.1\.4\b/fo1/g; s/10\.60\.1\.13\b/fo2/g; s/10\.60\.1\.9\b/fo3/g; s/10\.60\.1\.17\b/fo4/g; s/10\.60\.1\.15\b/fo5/g; s/10\.60\.1\.14\b/fo6/g; s/10\.60\.1\.8\b/foc/g; s/10\.60\.1\.16\b/fod/g'
find ~/merged -type f \( -name events -o -name roles -o -name 'nodes-*.txt' -o -name 'cluster-info-after.txt' -o -name target -o -name '*.out' -o -name summary.json -o -name create.log -o -name repl.csv -o -name converge.json \) -exec sed -i -E "$map" {} +
for f in $(find ~/merged -name 'logs-*.tgz'); do
  d=$(mktemp -d); n=$(basename "$f" | sed -E "$map")
  if ! tar xzf "$f" -C "$d" 2>/dev/null; then [ "$(basename "$f")" != "$n" ] && mv "$f" "$(dirname "$f")/$n"; rm -rf "$d"; continue; fi
  find "$d" -type f -exec sed -i -E "$map" {} +
  tar czf "$(dirname "$f")/$n.new" -C "$d" .; rm -f "$f"; mv "$(dirname "$f")/$n.new" "$(dirname "$f")/$n"; rm -rf "$d"
done
gzip -f $(find ~/merged -name repl.csv)
cd ~/rep
~/venv/bin/python h/fo_report.py ~/merged out
~/venv/bin/python h/cmp_rc4.py out/results/summary.csv rc4-summary.csv out > /dev/null
tar czf ~/merged.tgz -C ~ merged
echo MERGE-DONE
