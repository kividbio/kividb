# Memory suite harness

Everything here runs on Linux VMs: an engine VM and a client VM in one proximity placement group.
Nothing runs on the machine that drives the VMs except `az`, `ssh`/`scp` and `analyze.py`.

| file | runs on | what it does |
|---|---|---|
| `create-vms.sh` | operator | pair A: `mem-srv` Standard_E32ds_v7 + `mem-cli` Standard_D32ls_v7, one PPG |
| `create-vms-b.sh` | operator | pair B: `mem-srv2` Standard_E32ds_v6 + `mem-cli2` Standard_D16ls_v6 (second region, mixed runs) |
| `setup-common.sh` | both | Docker, redis-tools, python3, bun, warefeats/kv-stores at `a324d29` in `/opt/bench` |
| `setup-server.sh` | engine | mounts the local NVMe disk at `/mnt/nvme`, pulls the pinned images (`release` = the published `kividb/kividb:v1.2.0-rc4`) |
| `setup-client.sh` | client | builds warefeats' memtier image (memtier_benchmark 2.5.1) |
| `eng.sh` | engine | starts/stops one engine in Docker, host network, port 6379, exact flags inside |
| `sampler.py`, `mon.sh` | engine | once a second: cgroup anon/file memory, RSS of every process in the container, INFO used_memory, keys, snapshot flags, evictions |
| `warefeats.sh` | client | the warefeats harness unchanged (ssh to the engine VM in place of AWS SSM) |
| `growth.sh` | client | load 1M -> 5M -> 10M -> 25M -> 50M keys, checkpoint after each stage |
| `mixed.sh` | client | 10M keys, then 20 min of 80/20 GET/SET with resizing overwrites and TTLs at a fixed 150k ops/s |
| `snapshot.sh` | client | 30M x 1 KB, 100k SET/s; BGSAVE, then BGREWRITEAOF, each inside its own 420 s write window |
| `evict.sh` | client | maxmemory = used + 10%, allkeys-lru, 100k same-size overwrites/s, BGSAVE at +60 s (#284) |
| `queue.sh` | client | runs the lines of `/var/tmp/mem/queue.txt` one at a time |
| `analyze.py`, `make-report.sh`, `compact.py` | operator | tables and SVG charts from the raw outputs; compact copies of the raw files for results/ |

Reproduce (pair A), with `KEY` an SSH key pair whose public half is `KEY.pub`:

```
bash create-vms.sh kividb-rc4-memory germanywestcentral KEY.pub
# copy eng.sh mon.sh sampler.py setup-*.sh to the engine VM's /opt/mem, and the client scripts to the client's /opt/mem;
# give the client an ssh key (~/.ssh/id_mem) that the engine VM accepts
engine$ sudo bash /opt/mem/setup-common.sh && sudo bash /opt/mem/setup-server.sh release
client$ sudo bash /opt/mem/setup-common.sh && sudo bash /opt/mem/setup-client.sh
client$ cd /opt/mem
client$ ./warefeats.sh 10.30.0.4 25 full redis valkey dragonfly kividb-1.1.3 kividb-rc4 kividb-rc4-run2
client$ for e in kividb-rc4 kividb-1.1.3 redis valkey dragonfly; do for v in 100 1024; do
          ./growth.sh $e $v 1000000,5000000,10000000,25000000,50000000; done; done
client$ for e in kividb-rc4 kividb-1.1.3 redis valkey dragonfly; do ./snapshot.sh $e; done
client$ for e in kividb-rc4 kividb-1.1.3 redis; do ./evict.sh $e; done
client$ for e in kividb-rc4 redis valkey dragonfly kividb-1.1.3; do ./mixed.sh $e; done
# copy the client's /var/tmp/mem and /var/tmp/wf back, then
# copies go to raw/a (pair A), raw/b (pair B), raw/a2 and raw/b2 (snapshot reruns), raw/wf; then
bash make-report.sh   # PY=python on Windows
az group delete -n kividb-rc4-memory --yes
```
