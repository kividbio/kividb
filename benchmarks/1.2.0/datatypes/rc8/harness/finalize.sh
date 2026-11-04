#!/usr/bin/env bash
# on srv after session-rc8.sh: tables, the regression chart and the raw outputs in ~/final
set -eu
cd ~
rm -rf final; mkdir -p final/results/raw final/charts
python3 harness/summarize.py out/datatypes final/results
MPLBACKEND=Agg python3 harness/vs_rc7.py final/results final/charts > final/results/vs-rc7.md
python3 harness/compare.py final/results/summary.csv harness/pub-summary.csv > final/results/vs-published.md
cp lpop-tail.log final/results/lpop-tail.txt
cp master.log final/results/session.log
for d in out/datatypes/*/; do
  r=$(basename "$d"); mkdir -p "final/results/raw/$r"
  cp "$d/server-cpu.csv" "$d/versions.txt" "final/results/raw/$r/"
  rm -rf /tmp/scrub; mkdir -p /tmp/scrub; cp "$d"/*.json "$d"/*.txt /tmp/scrub/
  sed -i -e 's/10\.0\.0\.4/<server>/g' -e 's/10\.0\.0\.5/<client>/g' /tmp/scrub/*
  (cd /tmp/scrub && tar czf ~/final/results/raw/"$r"/memtier-reports.tgz ./*.json ./*.txt)
done
sed -i -e 's/10\.0\.0\.4/<server>/g' -e 's/10\.0\.0\.5/<client>/g' -e 's#/home/azureuser#~#g' final/results/*.txt final/results/*.log 2>/dev/null || true
{ echo "# builds"; cat build.log | grep -vE "Compiling|Finished"; echo; echo "# competitors"; ~/bin/redis-server --version
  docker images --format '{{.Repository}}:{{.Tag}} {{.ID}}' | grep -v kvrocks; ssh 10.0.0.5 'memtier_benchmark --version | head -1'
  uname -r; lscpu | grep 'Model name'; nproc; } > final/results/versions.txt
cp harness/session-rc8.sh harness/vs_rc7.py harness/compare.py final/
tar czf final.tgz final
echo FINAL-OK
