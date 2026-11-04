# Memory suite harness

Everything here runs on Linux VMs: an engine VM and a client VM in one proximity placement group.
Nothing runs on the machine that drives the VMs except `az`, `ssh`/`scp` and `analyze.py`.

| file | runs on | what it does |
|---|---|---|
| `create-vms.sh` | operator | pair A: `mem-srv` Standard_E32ds_v7 + `mem-cli` Standard_D32ls_v7, one PPG |
| `create-vms-b.sh` | operator | pair B: `mem-srv2` Standard_E32ds_v6 + `mem-cli2` Standard_D16ls_v6 (second region: mixed runs, active defrag) |
| `setup-common.sh` | both | Docker, redis-tools, python3, bun, warefeats/kv-stores at `a324d29` in `/opt/bench` |
| `setup-server.sh` | engine | mounts the local NVMe disk at `/mnt/nvme`, pulls the pinned competitor images, builds each KiviDB under test from a git ref with the repo's Dockerfile on the VM itself |
| `setup-client.sh` | client | builds warefeats' memtier image (memtier_benchmark 2.5.1) |
| `eng.sh` | engine | starts/stops one engine in Docker, host network, port 6379, exact flags inside |
| `sampler.py`, `mon.sh` | engine | once a second: cgroup anon/file memory, RSS of every process in the container, INFO used_memory, keys, snapshot flags, evictions, KiviDB's slab bytes and active-defrag state |
| `warefeats.sh` | client | the warefeats harness unchanged (ssh to the engine VM in place of AWS SSM) |
| `growth.sh` | client | load 1M -> 5M -> 10M -> 25M -> 50M keys, checkpoint after each stage |
| `mixed.sh` | client | 10M keys, then 20 min of 80/20 GET/SET over the whole keyspace (distinct client seeds) with resizing overwrites and TTLs at a fixed 150k ops/s |
| `snapshot.sh` | client | 30M x 1 KB, 100k uniformly random SET/s; BGSAVE, then BGREWRITEAOF, each inside its own write window |
| `evict.sh` | client | maxmemory = used + 10%, allkeys-lru, 100k same-size uniformly random overwrites/s, BGSAVE at +60 s (#284) |
| `defrag.sh` | client | 5M x 100 B, a fixed 80k GET/s + 20k SET/s probe load, delete a random half (seeded), watch slab bytes, RSS and latency while active defrag gives the memory back |
| `aoflat.sh` | client | AOF on, no snapshot: 2M x 1 KB, 100k uniformly random SET/s for 180 s; `PERF=1` adds 30 s of `perf record -g` on the engine (the check of the snapshot runs' steady-state write p99) |
| `queue.sh` | client | runs the lines of `/var/tmp/mem/queue.txt` one at a time |
| `analyze.py`, `make-report.sh`, `compact.py` | operator | tables and SVG charts from the raw outputs; compact copies of the raw files for results/ |

Every driver takes `R=<round>` (appended to the run name, e.g. `snap-kividb-cand-r2`); `analyze.py` gives the
median and min-max over rounds.

Reproduce, with `KEY` an SSH key pair whose public half is `KEY.pub`:

```
bash create-vms.sh kividb-rc8-memory germanywestcentral KEY.pub
bash create-vms-b.sh kividb-rc8-memory polandcentral KEY.pub
# copy *.sh *.py to /opt/mem on all four VMs; give each client an ssh key (~/.ssh/id_mem) that its engine VM accepts
engine$ sudo bash /opt/mem/setup-common.sh && sudo bash /opt/mem/setup-server.sh kividb-cand=75482abb kividb-rc4=v1.2.0-rc4
client$ sudo bash /opt/mem/setup-common.sh && sudo bash /opt/mem/setup-client.sh
# pair A client, in /opt/mem (rounds alternate candidate and rc4; r1 also runs every other engine):
./warefeats.sh 10.30.0.4 25 full kividb-cand kividb-rc4            # then kividb-rc4-run2 kividb-cand-run2, ...-run3
./warefeats.sh 10.30.0.4 25 full redis valkey dragonfly kividb-1.1.3
for e in kividb-cand kividb-rc4 redis valkey dragonfly kividb-1.1.3; do for v in 100 1024; do
  R=r1 ./growth.sh $e $v 1000000,5000000,10000000,25000000,50000000; done; done
for e in kividb-cand kividb-rc4 redis valkey dragonfly kividb-1.1.3; do R=r1 WIN=300 PRE=30 ./snapshot.sh $e; done
for e in kividb-cand kividb-rc4 kividb-1.1.3 redis; do R=r1 ./evict.sh $e; done
# then rounds r2 (rc4 first) and r3 (candidate first) of growth, snapshot and evict for kividb-cand and kividb-rc4
# pair B client:
for e in kividb-cand kividb-rc4 redis valkey dragonfly kividb-1.1.3; do SRV=10.31.0.4 R=r1 ./mixed.sh $e; done
# rounds r2, r3 of mixed for kividb-rc4 and kividb-cand, alternating
# active defrag: r1-r4 for kividb-cand and kividb-rc4, alternating, plus an r5 of kividb-cand and the controls
# (the candidate's r1 ran with the default PROBE=600; compaction is over within 15 s of the delete either way)
for r in r1 r2 r3 r4; do for e in kividb-cand kividb-rc4; do SRV=10.31.0.4 R=$r PROBE=300 ./defrag.sh $e; done; done
for r in r1 r2 r3; do SRV=10.31.0.4 R=$r TAG=off PROBE=300 ./defrag.sh kividb-cand 5000000 100 --activedefrag no; done
SRV=10.31.0.4 R=r2 TAG=off PROBE=300 ./defrag.sh kividb-rc4 5000000 100 --activedefrag no
# AOF-on write latency check: four alternating rounds, then one profiled round each
for r in r1 r2 r3 r4; do for e in kividb-cand kividb-rc4; do SRV=10.31.0.4 R=$r ./aoflat.sh $e; done; done
for e in kividb-cand kividb-rc4; do SRV=10.31.0.4 R=p1 PERF=1 ./aoflat.sh $e; done
# copy pair A's /var/tmp/mem to raw/a and /var/tmp/wf to raw/wf, pair B's /var/tmp/mem to raw/b; then
bash make-report.sh   # PY=python on Windows
az group delete -n kividb-rc8-memory --yes
```
