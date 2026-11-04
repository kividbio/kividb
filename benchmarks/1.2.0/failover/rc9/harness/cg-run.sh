#!/usr/bin/env bash
# cg-run.sh: rc8 and rc9 (the release binaries from build.sh: fat LTO, as shipped), alternating, two runs each,
# on an otherwise idle VM. uring, N 60000, P 16. Lines "<workload> <tag> <instructions per command>" in ~/cg.txt.
sudo mkdir -p /out /target/perf-rc8/release /target/perf-rc9/release; sudo chown -R azureuser /out /target
cp ~/kb-rc8/kividb /target/perf-rc8/release/kividb; cp ~/kb-rc9/kividb /target/perf-rc9/release/kividb
: > /out/raw.txt
for r in 1 2; do for t in rc8 rc9; do N=60000 P=16 MODEL=uring bash ~/cg-measure.sh $t | sed "s/ $t / $t run$r /"; done; done > ~/cg.txt 2>&1
echo CG-DONE >> ~/cg.txt
