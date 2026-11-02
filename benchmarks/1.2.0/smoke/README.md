# Smoke test of every published 1.2.0-rc7 artifact

**What this answers:** does what we published for rc7 actually work, on both CPU architectures, from both
registries, as a container, a tarball and an OS package? These are the files a user downloads, not the
source tree.

**Result: every artifact passes. The rc4 harness, unchanged: 36 of 36 artifact runs pass (2,238 checks,
0 failures), the same per-artifact counts as rc4.** The new rc7 checks pass too: 144 per architecture.
The one failure is the known rpm packaging issue from rc4, which rc7 does not fix: the `.rpm` does not
install on an image without `shadow-utils` (`amazonlinux:2023` minimal). Details are below.

Build under test: tag `v1.2.0-rc7`, commit `598b74d2`, release run
[41509100399](https://github.com/kividbio/kividb/actions/runs/41509100399). The release run succeeded.
For an RC, darwin, publish and homebrew are skipped by design.

## Summary per check

| Check | amd64 | arm64 |
|---|---|---|
| Binaries (ORAS artifact, 4 variants): start, `--version` and `INFO server` = `1.2.0-rc7`, PING/SET/GET, AOF kill -9 replay, short redis-benchmark | PASS (4/4) | PASS (4/4) |
| `.deb`: `dpkg -i` enables and starts the unit; the server runs as user `kividb`; it serves, benchmarks, and survives `systemctl kill -s KILL` with its data; purge stops it | PASS | PASS |
| `.rpm` under systemd (UBI 9 init container): `dnf install` enables and starts the unit as `kividb`; it serves; `dnf remove` stops it | PASS | PASS |
| `.rpm` on clean rocky 9, alma 9, fedora 41, ubi9, ubi9-minimal (`rpm -i`) | PASS | PASS |
| `.rpm` on clean `amazonlinux:2023` (no `shadow-utils`) | **FAIL, known issue (rc4), still open** | **same** |
| Images from GHCR, 4 variants: pull, run, serve, version, persistence, redis-benchmark | PASS | PASS |
| Images from Docker Hub, 4 variants: same checks | PASS | PASS |
| Docker Hub digests identical to GHCR, for every variant | PASS | PASS |
| Multi-arch tags resolve to the host's architecture (both registries) | PASS | PASS |
| `-amd64`/`-arm64` tags are their architecture (image config, `file` on the binary, no other-arch manifest) | PASS, all 8 tags | PASS, all 8 tags |
| Lua: `EVAL`/`EVALSHA`/`FCALL` work on lua/full; default and tls refuse `EVAL` with a clear `ERR`, and the connection keeps serving | PASS | PASS |
| TLS on tls/full: mutual TLS with a throwaway CA, **and** a plain self-signed cert with no client cert; plaintext refused on the TLS port; redis-benchmark over TLS | PASS | PASS |
| Builds without TLS refuse to start with `--tls-port` | PASS | PASS |
| `SHA256SUMS.txt` matches all 12 files | PASS (12/12) | PASS (12/12) |
| Signed build provenance (`gh attestation verify`): 12 files and 4 images, all signed at `refs/tags/v1.2.0-rc7`, commit 598b74d2, run 41509100399 | PASS (16/16) | (one check) |
| Repo `docker/verify/verify-published.sh` | PASS 16/16 | PASS 16/16 |
| Repo `smoke/replication_blackbox.sh` | PASS 19/19 | PASS 19/19 |
| Repo `tests/e2e` against the published default image, RESP2 and RESP3 | PASS: 1,171 passed, 65 skipped, 0 failed (each) | PASS: the same |

## What was tested

An RC gets no GitHub Release: its binaries and packages are the OCI artifact
`ghcr.io/kividbio/kividb-binaries:v1.2.0-rc7` (13 files: 8 tarballs, 2 `.deb`, 2 `.rpm`, `SHA256SUMS.txt`).
Its images are on GHCR and Docker Hub.

| Artifact | Where | Tested on |
|---|---|---|
| Images `v1.2.0-rc7`, `-lua`, `-tls`, `-full` (multi-arch) | `ghcr.io/kividbio/kividb` | amd64 and arm64 |
| The same four tags | `docker.io/kividb/kividb` | amd64 and arm64 |
| Per-architecture tags `v1.2.0-rc7[-variant]-amd64` / `-arm64` | GHCR only (by design) | run on their own arch; config and binary checked on both hosts |
| Tarballs `kividb[-lua\|-tls\|-full]-linux-{x86_64,aarch64}.tar.gz` | ORAS artifact | their own architecture |
| `kividb_{amd64,arm64}.deb` | ORAS artifact | Ubuntu 24.04 host, systemd |
| `kividb.{x86_64,aarch64}.rpm` | ORAS artifact | `amazonlinux:2023` (the rc4 path), 6 clean rpm images, UBI 9 init with systemd |

### 1. The rc4 harness, unchanged (`harness/smoke_all.sh`, `harness/smoke_artifact.sh`)

These are the repo's `benchmarks/1.2.0/smoke/harness/` files on `release/1.2.0`, run against `v1.2.0-rc7`.
Each artifact starts with the AOF on and then goes through:

- version;
- the `INFO all` sections;
- every data type: strings (with a 1 MB value), expiry, hash, list, set, zset, stream, HLL, bitmap, geo,
  JSON, MULTI/EXEC, vector set, FT search;
- Lua, or its refusal;
- TLS, or its refusal;
- 20,000 keys, `kill -9`, an AOF replay with a fingerprint match, then a clean shutdown and a snapshot load.

The full table is in `results/table.md`. Every row is PASS.

| Kind | Variant | Checks per run (rc4 → rc7) | Runs (amd64 + arm64) | Result |
|---|---|---|---|---|
| image, GHCR multi-arch / Docker Hub / GHCR per-arch | default | 59 → 59 | 6 | PASS |
| same | lua | 63 → 63 | 6 | PASS |
| same | tls | 62 → 62 | 6 | PASS |
| same | full | 66 → 66 | 6 | PASS |
| tarball | default / lua / tls / full | 59 / 63 / 62 / 66, unchanged | 8 | PASS |
| `.deb` (systemd, `Restart=on-failure` after a SIGKILL) | default | 59 → 59 | 2 | PASS |
| `.rpm` (amazonlinux:2023 + shadow-utils) | default | 60 → 60 | 2 | PASS |
| **Total** | | **2,238 → 2,238 checks** | **36** | **36 PASS** |

### 2. New checks for rc7 (`harness/extra_checks.sh`, `harness/followup.sh`, `harness/attest_all.sh`)

**Architecture of every tag** (`results/*/extra/arch.log`). On each host:

- each of the 8 multi-arch refs (4 variants × 2 registries) pulls as the host's `linux/<arch>`;
- each of the 8 per-arch GHCR tags is pulled for its own platform. Its image config says that
  architecture, `file` on `/usr/local/bin/kividb` says `x86-64` or `ARM aarch64`, and its raw manifest
  has no manifest for the other architecture.

This is 16 of 16 on both hosts.

**Variant features, checked exactly.**

- The default and tls builds answer `EVAL` with `ERR this build of KiviDB has no Lua: EVAL, SCRIPT,
  FUNCTION and FCALL need the lua or full variant (Docker image kividb/kividb:v1.2.0-rc7-lua, or a
  kividb-lua release file)`. A `PING` on the same connection then answers `PONG`.
- lua/full return the script's value.
- tls/full were also started with a **self-signed certificate** (no CA) and `--tls-auth-clients no`. In
  that setup:
  - `redis-cli --tls --cacert self.crt` gets `PONG` and SET/GET work;
  - plaintext on the TLS port is refused;
  - a client that does not trust the cert fails verification.

**The packages as services.**

- `.deb`:
  - the unit's MainPID runs as `kividb` (a system user with `/usr/sbin/nologin`);
  - `/var/lib/kividb` is owned by `kividb`, and `SAVE` writes `dump.kdb` there next to `appendonly.aof`;
  - `dpkg --purge` stops it.
- `.rpm`, in `registry.access.redhat.com/ubi9/ubi-init` with systemd as PID 1:
  - `dnf install` runs postinstall, which enables and starts the unit;
  - the MainPID runs as `kividb`;
  - version, PING/SET/GET, the EVAL refusal and a redis-benchmark all pass;
  - `dnf remove` stops it.

**The rpm on clean images, with nothing pre-installed** (`results/*/extra/rpm.log`, `rpm-ubi-minimal.log`):

| Image | `useradd` present? | Result |
|---|---|---|
| `amazonlinux:2023` | no | **FAIL**: `groupadd: command not found`, `Error in PREIN scriptlet` (the rc4 finding) |
| `rockylinux/rockylinux:9` | yes | PASS |
| `almalinux:9` | yes | PASS |
| `fedora:41` | yes | PASS |
| `ubi9/ubi` | yes | PASS |
| `ubi9/ubi-minimal` | yes | PASS with `rpm -i`*. `useradd` warns that `/usr/sbin/nologin` is missing; harmless. |

\* The first pass used `microdnf install /tmp/kividb.rpm`, and microdnf cannot install a local file
("No package matches"). That was a harness fault; `followup.sh` reran it with `rpm -i`. So the raw
`extra/result.txt` reads 143 pass / 2 fail per arch, and the corrected count is 144 / 1, the 1 being
amazonlinux.

**redis-benchmark** (`results/*/extra/bench.tsv`, chart `charts/bench-set-get.svg`). Settings: `-t
ping_mbulk,set,get,incr,lpush,lpop -n 100000 -c 50 -r 10000 -d 64`, client on the same 4-vCPU VM. Every
artifact completed all six tests with non-zero throughput, over plain TCP and, for tls/full, over TLS.
This is a liveness check, not a benchmark. Typical SET rates:

| | amd64 (D4s_v5) | arm64 (D4ps_v5) |
|---|---|---|
| tarballs, plain | 93k–111k | 59k–62k |
| images (docker-proxy port), plain | 84k–89k | 53k–58k |
| `.deb` / `.rpm` | 109k / 106k | 60k / 56k |
| TLS (tarball / image) | 62k–80k / 56k | 38k–39k / 33k–34k |

Images run behind Docker's userland port proxy, which costs some throughput against the bare tarballs;
that is expected.

**Registries** (`results/registry-tags.txt`, `results/*/manifests.txt`):

- Each multi-arch tag lists `linux/amd64` and `linux/arm64`, plus `unknown/unknown` attestation
  manifests.
- Docker Hub's digest equals GHCR's for every variant: default `aa2f305f…`, lua `6c83bae2…`,
  tls `badab69d…`, full `721cea8f…`.
- The floating tags did not move: `latest` and `1.1` are still `v1.1.3` (`5677cfc5…`) on both
  registries, and there is no `1.2` tag.
- The per-arch tags exist on GHCR only, as `release.yml` intends.

**Checksums and signatures** (`results/attestations.txt`, from the laptop with `oras` and `gh`, no Docker):

- `sha256sum -c SHA256SUMS.txt`: 12/12 OK, and again on each VM (`results/*/binaries-sha256.txt`).
- `gh attestation verify --repo kividbio/kividb`: 12/12 files and the 4 multi-arch GHCR images.
- Every attestation is signed by `release.yml` at `refs/tags/v1.2.0-rc7`, source commit `598b74d20ec0`,
  run `41509100399/attempts/1`.

## Comparison with rc4

| | rc4 | rc7 |
|---|---|---|
| Harness artifact runs | 36/36 PASS, 2,238 checks | 36/36 PASS, 2,238 checks (identical per-artifact counts) |
| `verify-published.sh` | 16/16 | 16/16 (both arches) |
| `replication_blackbox.sh` | 19/19 | 19/19 (both arches) |
| e2e published image, RESP2 / RESP3 | 1,171 passed, 65 skipped (arm64) | 1,171 passed, 65 skipped, on **both** arm64 and amd64 |
| rpm without shadow-utils | fails (note) | **still fails**: `packaging/nfpm.yaml` at rc7 declares no dependency; `rpm -qpR` lists nothing |
| Hub = GHCR digests; floating tags unmoved | yes | yes |
| Attestations verified | not checked | 16/16 |

No regression against rc4.

## Finding (known, still open): the `.rpm` needs `shadow-utils` but does not declare it

Repro, on any Docker host:

```bash
oras pull ghcr.io/kividbio/kividb-binaries:v1.2.0-rc7
docker run --rm -v "$PWD/kividb.x86_64.rpm":/tmp/k.rpm:ro amazonlinux:2023 dnf install -y /tmp/k.rpm
# -> /var/tmp/rpm-tmp.XXXX: line 7: groupadd: command not found
#    error: %prein(kividb-1.2.0~rc7-1.x86_64) scriptlet failed, exit status 127
```

`packaging/scripts/preinstall.sh` runs `groupadd`/`useradd`, and the package declares no requirements
(`rpm -qpR` prints nothing). Full RHEL, Rocky, Alma, Fedora and UBI hosts have `shadow-utils`, and the
package installs there. Minimal images such as `amazonlinux:2023` do not.

The fix suggested at rc4 still applies: in `packaging/nfpm.yaml`,
`overrides: { rpm: { depends: [shadow-utils] } }`. It could also be a `Requires(pre)`, but nfpm's
`depends` is the simple route. The `.deb` is not affected.

## Hardware and software

- Azure, region **Central India**, RG `kividb-rc7-smoke`. It was deleted after the run, and
  `az group exists` returns `false`.
- amd64: `Standard_D4s_v5` (4 vCPU, 16 GB). arm64: `Standard_D4ps_v5` (4 vCPU Ampere Altra, 16 GB).
- Ubuntu 24.04 LTS on both, with Docker 29.1.3 (+ buildx, compose v2 2.40.3), redis-tools 7.0, ORAS
  1.2.2.
- The server reports `multiplexing_api:io_uring` on both.
- The repo was at `release/1.2.0` = `598b74d2`, the tag itself.

## Reproduce

On an Ubuntu 24.04 amd64 host and on an arm64 host:

```bash
bash harness/vm_setup.sh                              # docker, redis-tools, oras, repo at release/1.2.0
sudo apt-get install -y docker-compose-v2             # verify-published.sh needs it (missed in vm_setup at first)
bash harness/run_rc7.sh v1.2.0-rc7                    # smoke_all.sh (rc4 harness) + e2e + extra_checks.sh
bash harness/followup.sh v1.2.0-rc7                   # verify-published rerun, rpm on ubi9-minimal with rpm -i
python3 harness/smoke_table.py ~/smoke-results/*/summary.tsv
```

Then run these on any machine with `gh` and `oras`, no Docker needed:

```bash
mkdir dl && (cd dl && oras pull ghcr.io/kividbio/kividb-binaries:v1.2.0-rc7)
bash harness/attest_all.sh v1.2.0-rc7 dl
python3 harness/bench_chart.py results/amd64/extra/bench.tsv results/arm64/extra/bench.tsv charts/bench-set-get.svg
```

Logs: `results/<arch>/<artifact>.log` (rc4 harness) and `results/<arch>/extra/*.log` (new checks). Each
has one `PASS:`/`FAIL:` line per check.
