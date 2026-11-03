#!/usr/bin/env bash
# on srv after session-rc10.sh: tables, the regression chart and the raw outputs in ~/final
set -eu
cd ~
rm -rf final; mkdir -p final/results/raw final/charts
python3 harness/summarize.py out/datatypes final/results
MPLBACKEND=Agg python3 harness/vs_rc8.py final/results final/charts harness/rc8-vs-rc7.csv > final/results/vs-rc8.md
python3 harness/compare_rc8.py final/results/summary.csv harness/rc8-summary.csv > final/results/vs-rc8-round.md
cp master.log final/results/session.log
for d in out/datatypes/*/; do
  r=$(basename "$d"); mkdir -p "final/results/raw/$r"
  cp "$d/server-cpu.csv" "$d/versions.txt" "final/results/raw/$r/"
  rm -rf /tmp/scrub; mkdir -p /tmp/scrub; cp "$d"/*.json "$d"/*.txt /tmp/scrub/
  sed -i -e 's/10\.0\.0\.4/<server>/g' -e 's/10\.0\.0\.5/<client>/g' /tmp/scrub/*
  (cd /tmp/scrub && tar czf ~/final/results/raw/"$r"/memtier-reports.tgz ./*.json ./*.txt)
done
sed -i -e 's/10\.0\.0\.4/<server>/g' -e 's/10\.0\.0\.5/<client>/g' -e 's#/home/azureuser#~#g' final/results/*.txt final/results/*.log final/results/raw/*/*.txt 2>/dev/null || true
{ echo "# builds"; grep -vE "Compiling|Finished" build.log; echo; echo "# client"; ssh 10.0.0.5 'memtier_benchmark --version | head -1'
  uname -r; lscpu | grep 'Model name'; nproc; } > final/results/versions.txt
sed -i -e 's#/home/azureuser#~#g' final/results/versions.txt
cp harness/session-rc10.sh harness/vs_rc8.py harness/compare_rc8.py harness/finalize-rc10.sh final/
tar czf final.tgz final
echo FINAL-OK
