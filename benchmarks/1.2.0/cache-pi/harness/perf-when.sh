#!/bin/bash
# perf-when.sh <engine> <tag>   (runs on a server VM under nohup)
# Waits until server-run.sh has started <engine>, lets the harness get into its measured phase,
# then samples the server with perf for 30 s and writes flat and inclusive reports.
ENGINE=$1; TAG=$2
until grep -q "^engine: $ENGINE\$" ~/run/cmdline.txt 2>/dev/null && kill -0 "$(cat ~/run/pid)" 2>/dev/null; do sleep 5; done
PID=$(cat ~/run/pid)
sleep 190
cd ~/run
sudo perf record -F 999 -g -p "$PID" -o perf-$TAG.data -- sleep 30 2> perf-$TAG.rec.txt
sudo perf report -i perf-$TAG.data --no-children --sort symbol --stdio 2>/dev/null | grep -v '^#' | grep '%' | head -40 > perf-$TAG-self.txt
sudo perf report -i perf-$TAG.data --children --sort symbol --stdio -g none 2>/dev/null | grep -v '^#' | grep '%' | head -60 > perf-$TAG-children.txt
echo done > perf-$TAG.done
