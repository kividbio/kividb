#!/usr/bin/env bash
# on srv after session.sh: tables, the comparison chart and the raw outputs in ~/final
# (harness/published-summary.csv = benchmarks/1.2.0/datatypes/results/summary.csv, copied over)
set -eu
cd ~
rm -rf final; mkdir -p final/results/raw final/charts
python3 harness/summarize.py out/datatypes final/results
MPLBACKEND=Agg python3 harness/vs_120.py final/results final/charts harness/published-summary.csv out/datatypes > final/results/vs-120.md
cp master.log final/results/session.log
for d in out/datatypes/*/; do
  r=$(basename "$d"); mkdir -p "final/results/raw/$r"
  cp "$d/server-cpu.csv" "$d/versions.txt" "$d/expiry.csv" "final/results/raw/$r/"
  rm -rf /tmp/scrub; mkdir -p /tmp/scrub; cp "$d"/*.json "$d"/*.txt /tmp/scrub/
  sed -i -e 's/10\.0\.0\.4/<server>/g' -e 's/10\.0\.0\.5/<client>/g' /tmp/scrub/*
  (cd /tmp/scrub && tar czf ~/final/results/raw/"$r"/memtier-reports.tgz ./*.json ./*.txt)
done
for r in out/log-*.txt; do grep -E "HFE|WARN|failed" "$r" | sed "s#^#$(basename "$r" .txt): #"; done > final/results/hfe-checks.txt
sed -i -e 's/10\.0\.0\.4/<server>/g' -e 's/10\.0\.0\.5/<client>/g' -e 's#/home/azureuser#~#g' final/results/*.txt final/results/*.log final/results/raw/*/*.txt 2>/dev/null || true
{ echo "# builds"; grep -vE "Compiling|Downloaded|Downloading|Pulling|Pull complete|Download complete|^info:" build.log; echo; echo "# client"
  ssh 10.0.0.5 'memtier_benchmark --version | head -1; uname -r; lscpu | grep "Model name"; nproc'
  echo "# server"; uname -r; lscpu | grep 'Model name'; nproc; docker --version; } > final/results/versions.txt
sed -i -e 's#/home/azureuser#~#g' final/results/versions.txt
tar czf final.tgz final
echo FINAL-OK
