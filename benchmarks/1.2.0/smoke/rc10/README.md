# Smoke test of every published 1.2.0-rc10 artifact

**What this answers:** does what we published for rc10 work on both CPU architectures, from both
registries, as a container, a tarball and an OS package? It is the last check before 1.2.0 is tagged.
These are the files a user downloads, not the source tree.

**Result: every artifact passes every check. No bug found.**
- The rc4 harness, unchanged, gives 36 of 36 artifact runs PASS (2,238 checks, 0 failures), the same
  per-artifact counts as rc4 and rc7.
- The rc7 extra checks give 145/0 per architecture. rc7 had one failure here, the `.rpm` on
  `amazonlinux:2023`, and it is now fixed.
- The new rc10 checks give 101/0 per architecture: `kividb.lock`, systemd start/restart/stop, and the
  new `repl-backlog-size` and `shutdown-timeout` defaults.

Build under test: tag `v1.2.0-rc10`, commit `39bfacd2`, release run
[41650715612](https://github.com/kividbio/kividb/actions/runs/41650715612). The release run succeeded.
Every artifact reports version `1.2.0-rc10`.

## Summary per check

| Check | amd64 | arm64 |
|---|---|---|
| Binaries (ORAS artifact, 4 variants): start; `--version` and `INFO server` give `1.2.0-rc10`; PING/SET/GET; AOF kill -9 replay; a short redis-benchmark | PASS (4/4) | PASS (4/4) |
| `.deb`: `dpkg -i` enables and starts the unit; it runs as user `kividb`; serves; survives `systemctl kill -s KILL` with its data; purge stops it | PASS | PASS |
| `.rpm` under systemd (UBI 9 init): `dnf install` enables and starts the unit as `kividb`; serves; `dnf remove` stops it | PASS | PASS |
| **`.rpm` on clean `amazonlinux:2023` (no `shadow-utils` before), `dnf install`** (failed on rc7) | **PASS**: dnf pulls in `shadow-utils-4.9-12`, creates user `kividb`, and the server answers PING | **PASS** |
| `.rpm` on clean rocky 9, alma 9, fedora 41, ubi9, ubi9-minimal | PASS (5/5) | PASS (5/5) |
| **`kividb.lock`, tarball:** a second server in the same dir waits, logs "in use" and does not serve. It takes over with all keys after the first's SHUTDOWN, and again after a kill -9 | PASS | PASS |
| **`kividb.lock`, image:** a second container on the same volume waits and logs "in use", then takes over with all keys after `docker stop`. `docker stop` exits 0 in 0.25 s | PASS | PASS |
| **`.deb` unit + lock:** start; `systemctl restart` ×5 (0.05–0.06 s each, never hangs, keys kept, no restart waits for the lock); stop (0.05 s, `Result=success`); start; SIGKILL then `Restart=on-failure` | PASS | PASS |
| **`.rpm` unit + lock (ubi9-init):** the same | PASS (restart 0.07–0.08 s, stop 0.07 s) | PASS (0.08–0.09 s, stop 0.08 s) |
| **Second server by hand (as `kividb`) beside the running unit:** logs "is in use by another process (it holds kividb.lock): waiting", does not serve, and the unit keeps serving. After `systemctl stop` it takes over with all keys. The reverse also works: the unit waits while the hand-started server holds the dir, and comes up when it exits | PASS (deb and rpm) | PASS (deb and rpm) |
| **`CONFIG GET repl-backlog-size` = `67108864` (64 MB) and `shutdown-timeout` = `10`:** 4 tarballs, 4 GHCR images, 4 Docker Hub images, the `.deb` and the `.rpm` | PASS (14/14) | PASS (14/14) |
| Images from GHCR and Docker Hub, 4 variants each: pull, run, serve, version, persistence, redis-benchmark | PASS | PASS |
| Docker Hub digests identical to GHCR, for every variant | PASS | PASS |
| Multi-arch tags resolve to the host's architecture; the `-amd64`/`-arm64` tags are their own architecture (config, `file`, no manifest for the other arch) | PASS (16/16) | PASS (16/16) |
| Lua on lua/full; default/tls refuse `EVAL` with a clear `ERR`, and the connection keeps serving | PASS | PASS |
| TLS on tls/full: mutual TLS, a plain self-signed cert, plaintext refused on the TLS port, redis-benchmark over TLS | PASS | PASS |
| Builds without TLS refuse to start with `--tls-port` | PASS | PASS |
| `SHA256SUMS.txt` matches all 12 files | PASS (12/12) | PASS (12/12) |
| Signed build provenance (`gh attestation verify`): 12 files and 4 images, all at `refs/tags/v1.2.0-rc10`, commit 39bfacd2, run 41650715612 | PASS (16/16) | (one check) |
| Repo `docker/verify/verify-published.sh` | PASS 16/16 | PASS 16/16 |
| Repo `smoke/replication_blackbox.sh` | PASS 19/19 | PASS 19/19 |
| Repo `tests/e2e` against the published default image, RESP2 and RESP3 | PASS: 1,171 passed, 65 skipped, 0 failed (each) | PASS: the same |

## What changed since rc7, and how it was checked (`harness/rc10_checks.sh`)

**The `.rpm` declares `shadow-utils`.**
- `rpm -qpR` now prints `shadow-utils`; at rc7 it printed nothing.
- On a clean `amazonlinux:2023` (`useradd` absent), `dnf install -y kividb.<arch>.rpm` installs
  `kividb-1.2.0~rc10-1` and `shadow-utils-4.9-12.amzn2023.0.4`.
- It creates `kividb:x:999:999:KiviDB:/var/lib/kividb:/usr/sbin/nologin`, and the server runs as that
  user and answers PING.
- The repo's own rpm smoke (`smoke_artifact.sh`) now prints "rpm installs on the minimal amazonlinux:2023
  image". All six clean rpm images pass.

**`kividb.lock` in the data directory** (`src/main.rs` `lock_data_dir()`: an exclusive `flock`, taken
after the banner and before anything in the directory is read; the waiting process retries every 50 ms
and logs every 5 s):

- **Tarball** (`results/<arch>/rc10/tarballs.log`, `tarball-second-server.log`):
  - Server A runs with the AOF on and 20,000 keys. Server B, on another port in the same directory,
    logs `The data directory <dir> is in use by another process (it holds kividb.lock): waiting for it to
    be free (0s so far)`. It does not answer PING and keeps waiting.
  - `SHUTDOWN` of A: B serves 0.4 s later, logs `The data directory is free after 3.2s: starting`, and
    has the 20,000 keys.
  - A third server C then waits on B. `kill -9` of B: C takes over and replays the AOF to the same key
    count. The kernel releases the lock however the holder dies.
- **Image** (`rc10/images.log`, `image-second-container.log`): two containers on one named volume give
  the same picture.
  - `docker stop` of the holder takes 0.25 s, exit code 0, well inside Docker's 10 s grace. With
    `shutdown-timeout 10` and no replicas, the server does not wait.
  - The second container takes over with all keys. `docker restart` is back in 0.64–0.66 s.
- **`.deb` on the Ubuntu host and `.rpm` in `ubi9-init`, both under systemd** (`rc10/deb.log`,
  `rc10/rpm-systemd.log`, plus `*-journal.log` and `*-second-server.log`):
  - The unit starts, and `/var/lib/kividb/kividb.lock` exists (owner `kividb`, mode 644).
  - With `appendonly yes` and 20,000 keys, five `systemctl restart`s each return in under 0.1 s and
    serve again with all keys. None hangs, and the journal shows no restart waiting for the lock: systemd
    waits for the old process to exit, so the lock is already free.
  - `systemctl stop` takes ≤ 0.08 s and ends with `Result=success`. `systemctl start` works.
    `systemctl kill -s KILL` is followed by `Restart=on-failure` bringing it back with all keys.
  - A server started by hand with `runuser -u kividb`, in `/var/lib/kividb` on port 6399, logs the
    "in use … waiting" line and does not serve. The unit keeps serving.
  - `systemctl stop` lets the hand-started server take over: "free after 3.1s", 20,000 keys.
  - `systemctl start` while the hand-started server holds the directory leaves the unit `active`
    (Type=simple) but waiting, with the wait line in its journal. It comes up when the other exits.

**New defaults:** `CONFIG GET repl-backlog-size` returns `67108864` and `CONFIG GET shutdown-timeout`
returns `10`. They were read on every tarball (4), every image from both registries (8), and on the
`.deb` and `.rpm` with their packaged config.

### Harness faults on the way (not product bugs; details in `NOTES.md`)

The first two passes of `rc10_checks.sh` failed for harness reasons. Their logs are kept as
`results/<arch>/rc10-firstpass/` and `rc10-secondpass/`; `results/<arch>/rc10/` is the clean third pass.

- **First pass, 19 failures per arch.** The script ran six `systemctl` starts within 10 s (install, AOF
  restart, restarts #1–#4). systemd's default `StartLimitBurst=5` / `StartLimitIntervalSec=10s` refuses
  a sixth start of any unit with `start-limit-hit`, and every later check in that block followed from the
  unit being down. Restarts #1–#3 had already returned in 0.05–0.08 s and served. Fixed by spacing
  starts 3 s apart.
  - Not a KiviDB change: the unit sets no StartLimit, the same as rc7.
  - An operator scripting more than five restarts in 10 s would hit this with any service.
- **Second pass, 1 failure per arch, on the `.deb` only.** "A restart had to wait for the lock" matched
  a journal line from 14:35, written during the first pass's deliberate hand-started-server step.
  The host journal outlives `dpkg --purge`. Inside the second pass's own window the journal showed no wait
  during the five restarts. Fixed with `journalctl --since <test start>`.
- **Before those: the script passed `--aof yes`.** The CLI flag takes no value, so the tarballs refused
  to start. Fixed to `--aof`.

## What was tested

An RC gets no GitHub Release: its binaries and packages are the OCI artifact
`ghcr.io/kividbio/kividb-binaries:v1.2.0-rc10` (13 files: 8 tarballs, 2 `.deb`, 2 `.rpm`, `SHA256SUMS.txt`).
Its images are on GHCR and Docker Hub.

| Artifact | Where | Tested on |
|---|---|---|
| Images `v1.2.0-rc10`, `-lua`, `-tls`, `-full` (multi-arch) | `ghcr.io/kividbio/kividb` | amd64 and arm64 |
| The same four tags | `docker.io/kividb/kividb` (pulled on the VMs) | amd64 and arm64 |
| Per-architecture tags `v1.2.0-rc10[-variant]-amd64` / `-arm64` | GHCR only (by design) | run on their own arch; config and binary checked on both hosts |
| Tarballs `kividb[-lua\|-tls\|-full]-linux-{x86_64,aarch64}.tar.gz` | ORAS artifact | their own architecture |
| `kividb_{amd64,arm64}.deb` | ORAS artifact | Ubuntu 24.04 host, systemd |
| `kividb.{x86_64,aarch64}.rpm` | ORAS artifact | `amazonlinux:2023` (clean and the rc4 path), 5 other clean rpm images, UBI 9 init with systemd |

1. **The rc4 harness, unchanged.** `benchmarks/1.2.0/smoke/harness/smoke_all.sh` and
   `smoke_artifact.sh`, from the repo at the tag. Each artifact starts with the AOF on and goes through
   the following, with the full table in `results/table.md`:
   - version and the `INFO all` sections;
   - every data type: strings (with a 1 MB value), expiry, hash, list, set, zset, stream, HLL, bitmap,
     geo, JSON, MULTI/EXEC, vector set, FT search;
   - Lua or its refusal, and TLS or its refusal;
   - 20,000 keys, `kill -9`, AOF replay with a fingerprint match, then a clean shutdown and a snapshot
     load.

   | Kind | Variant | Checks per run (rc7 → rc10) | Runs (amd64 + arm64) | Result |
   |---|---|---|---|---|
   | image: GHCR multi-arch / Docker Hub / GHCR per-arch | default / lua / tls / full | 59 / 63 / 62 / 66, unchanged | 24 | PASS |
   | tarball | default / lua / tls / full | 59 / 63 / 62 / 66, unchanged | 8 | PASS |
   | `.deb` (systemd) | default | 59 → 59 | 2 | PASS |
   | `.rpm` (amazonlinux:2023) | default | 60 → 60 | 2 | PASS |
   | **Total** | | **2,238 → 2,238** | **36** | **36 PASS** |

2. **The rc7 extra checks** (`harness/extra_checks.sh`), unchanged except that ubi9-minimal now uses
   `rpm -i`; microdnf cannot install a local file, a fault that rc7's `followup.sh` corrected by hand.
   They cover:
   - the architecture of every tag;
   - the exact EVAL refusal;
   - TLS with a self-signed cert;
   - the `.deb`/`.rpm` service user;
   - the rpm on six clean images;
   - redis-benchmark on every artifact.

   145 PASS / 0 FAIL per architecture. rc7 had 143/2 before its corrections.
3. **The repo's own published-image checks:**
   - `docker/verify/verify-published.sh`: 16/16;
   - `smoke/replication_blackbox.sh`: 19/19;
   - the `tests/e2e` suite against the published default image: 1,171 passed, 65 skipped, 0 failed, for
     RESP2 and RESP3 on both architectures.
4. **The new rc10 checks** (`harness/rc10_checks.sh`, above): 101/0 per architecture.
5. **From the laptop (no Docker):**
   - `sha256sum -c` 12/12;
   - `gh attestation verify` 16/16 (`results/attestations.txt`);
   - `oras manifest fetch` of every tag (`results/registry-tags.txt`).

**Registries:**
- Each multi-arch tag lists `linux/amd64` and `linux/arm64`, plus attestation manifests.
- Docker Hub's digest equals GHCR's for every variant: default `35f98bd7…`, lua `97738216…`,
  tls `b895ec23…`, full `c189b9b7…`.
- `latest` and `1.1` are still `v1.1.3` (`5677cfc5…`) on both registries, and there is no `1.2` tag
  yet. The per-arch tags are on GHCR only, as `release.yml` intends.

**redis-benchmark** (`results/<arch>/extra/bench.tsv`): `-t ping_mbulk,set,get,incr,lpush,lpop -n 100000
-c 50 -r 10000 -d 64`, with the client on the same 4-vCPU VM. Every artifact completed all six tests. This
is a liveness check, not a benchmark. The rates are in line with rc7:

| SET, requests/s | amd64 (D4s_v5) | arm64 (D4ps_v5) |
|---|---|---|
| tarballs, plain | 95k–111k | 55k–60k |
| images (docker-proxy port), plain | 79k–85k | 55k–59k |
| `.deb` / `.rpm` | 113k / 105k | 58k / 58k |
| TLS (tarball / image) | 79k–83k / 55k | 35k–40k / 38k–42k |

## Comparison with rc7

| | rc7 | rc10 |
|---|---|---|
| Harness artifact runs | 36/36 PASS, 2,238 checks | 36/36 PASS, 2,238 checks (identical per-artifact counts) |
| Extra checks per arch | 143/2 (corrected 144/1) | 145/0 |
| rpm on clean `amazonlinux:2023` | **fails** (`groupadd: command not found`) | **passes**: `shadow-utils` declared and pulled in |
| `kividb.lock` / systemd restart, stop, second server | n/a | all PASS; restart < 0.1 s, never hangs |
| `repl-backlog-size` / `shutdown-timeout` defaults | n/a | 67108864 / 10 on all 14 artifact kinds per arch |
| `verify-published.sh` / `replication_blackbox.sh` | 16/16 / 19/19 | 16/16 / 19/19 |
| e2e published image, RESP2 / RESP3 | 1,171 passed, 65 skipped | 1,171 passed, 65 skipped |
| Hub = GHCR digests; floating tags unmoved | yes | yes |
| Attestations verified | 16/16 | 16/16 |

No regression against rc7, and rc7's one open finding is fixed.

## Hardware and software

- Azure, region **Central India**, RG `kividb-rc10-smoke`. It was deleted after the run, and
  `az group exists` returns `false`.
- amd64: `Standard_D4s_v5` (4 vCPU, 16 GB). arm64: `Standard_D4ps_v5` (4 vCPU Ampere Altra, 16 GB).
- Ubuntu 24.04 LTS on both, with Docker 29.1.3 (+ buildx, compose v2), redis-tools 7.0, ORAS 1.2.2.
- The server reports `multiplexing_api:io_uring` on both.
- The repo was checked out at the tag `v1.2.0-rc10` = `39bfacd2`.

## Reproduce

On an Ubuntu 24.04 amd64 host and on an arm64 host:

```bash
bash harness/vm_setup.sh v1.2.0-rc10        # docker, compose, redis-tools, oras, repo at the tag
bash harness/run_rc10.sh v1.2.0-rc10        # rc10_checks.sh, smoke_all.sh (rc4 harness), e2e, extra_checks.sh
python3 harness/smoke_table.py ~/smoke-results/*/summary.tsv
```

`rc10_checks.sh` alone takes about 3.5 minutes and needs sudo (dpkg, systemctl) and Docker. Then, on any
machine with `gh` and `oras` and no Docker:

```bash
mkdir dl && (cd dl && oras pull ghcr.io/kividbio/kividb-binaries:v1.2.0-rc10)
bash harness/attest_all.sh v1.2.0-rc10 dl
```

Logs:
- `results/<arch>/<artifact>.log`: the rc4 harness;
- `results/<arch>/extra/*.log`: the rc7 extra checks;
- `results/<arch>/rc10/*.log`: the rc10 checks.

Each has one `PASS:`/`FAIL:` line per check.
