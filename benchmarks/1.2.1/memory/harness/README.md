# Memory harness, 1.2.1 round

The harness of the 1.2.0 memory suite's last round (pair A), with the build names changed, the two
builds prepared from the release tarball and from `release.yml`'s build, and one new driver (`hfe.sh`).
Everything runs on Linux VMs: an engine VM and a client VM in one proximity placement group ("pair A"). The machine
that drives the VMs runs only `az`, `ssh`/`scp` and `analyze121.py`.

| file | runs on | what it does |
|---|---|---|
| `create-vms.sh` | operator | `mem-srv` Standard_E32ds_v7 + `mem-cli` Standard_D16ls_v7, one PPG, SSH from one /32 only, auto-shutdown at +6 h |
| `setup-common.sh` | both | Docker, redis-tools, python3, bun, warefeats/kv-stores at `a324d29` in `/opt/bench` (unchanged) |
| `setup-server.sh` | engine | mounts the local NVMe disk at `/mnt/nvme`; downloads the 1.2.0 x86_64 release tarball and checks it against the release's `SHA256SUMS.txt`; builds 1.2.1 (878746e4) as `release.yml` does (amazonlinux:2023, rustup stable, `cargo build --release`, default features); wraps each binary in the same `ubuntu:24.04` image |
| `setup-client.sh` | client | builds warefeats' memtier image (memtier_benchmark 2.5.1) (unchanged) |
| `eng.sh` | engine | starts/stops one engine in Docker, host network, port 6379; records `repl-backlog-size` and the empty engine's `used_memory` / `used_memory_rss` at every start |
| `sampler.py`, `mon.sh` | engine | the 1-second memory sampler (unchanged) |
| `warefeats.sh` | client | the warefeats harness unchanged; `WF_ONLY=memory-load` / `memory-load-random` runs only the bytes-per-key cell |
| `growth.sh` | client | load 1M -> 5M -> 10M -> 25M -> 50M keys (random bytes), checkpoint after each stage (unchanged) |
| `hfe.sh` | client | new: 1M hashes x 10 fields x 16 B, then `HPEXPIRE` (10 h) on every field; checkpoints after each stage |
| `queue.sh`, `queue.txt` | client | the job queue and the exact list of jobs this round ran, in order |
| `analyze121.py` | operator | `results/tables.md` from the raw files |
| `compact.py` | operator | unchanged from 1.2.0 (not needed this round: the raw files are small) |

Reproduce, with `KEY` an SSH key pair whose public half is `KEY.pub`:

```
bash create-vms.sh <rg> koreacentral KEY.pub
# copy *.sh *.py to /opt/mem on both VMs and the source tree of 878746e4 (git archive, .tgz) to the engine VM;
# give the client an ssh key (~/.ssh/id_mem) that the engine VM accepts
engine$ sudo bash /opt/mem/setup-common.sh && sudo bash /opt/mem/setup-server.sh /var/tmp/src-878746e4.tgz
client$ sudo bash /opt/mem/setup-common.sh && sudo bash /opt/mem/setup-client.sh
client$ cp queue.txt /var/tmp/mem/queue.txt && cd /opt/mem && nohup ./queue.sh &
# copy the client's /var/tmp/mem to results/raw/mem and /var/tmp/wf to results/raw/wf, then
python3 -I harness/analyze121.py results/raw/mem results/raw/wf > results/tables.md
az group delete -n <rg> --yes
```
