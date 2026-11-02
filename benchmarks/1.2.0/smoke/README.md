# Smoke test of every published 1.2.0-rc4 artifact

**What this answers:** does what we published actually work, on both CPU architectures, from both
registries, as a container, a tarball and an OS package? Not the source tree: the files a user downloads.

**Result: 36 of 36 artifact runs pass (2,238 checks, 0 failures).** One packaging note: the `.rpm` does
not install on a *minimal* container image that lacks `shadow-utils` (details below); on a normal
Amazon Linux 2023 / RHEL host it installs and runs.

Build under test: tag `v1.2.0-rc4`, commit `a6b1bf38`, release run
[41357946103](https://github.com/kividbio/kividb/actions/runs/41357946103) (that run's own `smoke-test`
job, the repo's `smoke-test.yml`, was green too).

## What was tested

A release candidate gets no GitHub Release (by design, see `release.yml`): its binaries and packages
are an OCI artifact on GHCR, `ghcr.io/kividbio/kividb-binaries:v1.2.0-rc4` (`oras pull`), and its
images are on GHCR and Docker Hub.

| Artifact | Where | Tested on |
|---|---|---|
| Images `v1.2.0-rc4`, `-lua`, `-tls`, `-full` (multi-arch) | `ghcr.io/kividbio/kividb` | amd64 and arm64 |
| The same four tags | `docker.io/kividb/kividb` | amd64 and arm64 |
| Per-architecture source tags `v1.2.0-rc4[-variant]-amd64` / `-arm64` | GHCR only (by design) | their own architecture |
| Tarballs `kividb[-lua\|-tls\|-full]-linux-{x86_64,aarch64}.tar.gz` | ORAS artifact | their own architecture |
| `kividb_{amd64,arm64}.deb` (default variant, systemd unit) | ORAS artifact | Ubuntu 24.04 host, installed with `dpkg -i` |
| `kividb.{x86_64,aarch64}.rpm` (default variant) | ORAS artifact | `amazonlinux:2023` container, `dnf install` |

Registry facts checked (`results/*/manifests.txt`, `results/registry-tags.txt`):

* Each of the four multi-arch tags lists `linux/amd64` and `linux/arm64`, and **Docker Hub's digest is
  identical to GHCR's** for every variant (default `sha256:06ae4b82…`, lua `b9282101…`, tls `56cbc679…`,
  full `1eaf2428…`).
* The floating tags did not move for a candidate: `latest` and `1.1` are still `v1.1.3` on both
  registries; there is no `1.2` tag yet. The per-architecture tags exist on GHCR only, as `release.yml`
  intends.
* `SHA256SUMS.txt` in the ORAS artifact verifies all 12 files (`results/*/binaries-sha256.txt`).

### Checks per artifact (`harness/smoke_artifact.sh`)

Each artifact is started with the AOF on, then:

1. starts, answers `PING`; `--version` and `INFO server` `kividb_version` say `1.2.0-rc4`;
2. `INFO all` has the Server, Clients, Memory, Persistence, Stats, Replication, CPU and Keyspace sections,
   `aof_enabled:1`, `role:master`;
3. one or more commands of every type: strings (incl. a 1 MB value, `MSET`/`MGET`, `APPEND`, `INCRBY`),
   expiry (`EX` TTL, `PX` key gone), hash, list, set, sorted set, stream, HyperLogLog, bitmap, geo, JSON,
   `MULTI`/`EXEC`, vector set (`VADD`/`VSIM`), search (`FT.CREATE`, text / numeric / tag `FT.SEARCH`);
4. **Lua** (`lua`, `full`): `EVAL` with `redis.call`, `SCRIPT LOAD` + `EVALSHA`, `FUNCTION LOAD` + `FCALL`.
   Variants without Lua must refuse `EVAL`;
5. **TLS** (`tls`, `full`): mutual TLS with a throwaway CA, `PING` and `SET`/`GET` over TLS, a client
   without a certificate refused. Variants without TLS must refuse to start when given `--tls-port`;
6. **persistence:** 20,000 more keys, then `kill -9`, restart, `DBSIZE` and a fingerprint of every
   checked value must come back from the AOF; then a clean shutdown and restart (snapshot load), same
   fingerprint.

The `.deb` is checked the way an operator uses it: `dpkg -i` must enable and start the systemd unit;
the AOF is turned on in `/etc/kividb/kividb.conf`; the kill is `systemctl kill -s KILL`, and systemd's
`Restart=on-failure` must bring the server back by itself with the data.

The repo's own published-image checks also ran on both architectures (the `container-verify` and
`replication-blackbox` jobs of `smoke-test.yml`): `docker/verify/verify-published.sh` 16/16 (features
per variant, ACL enforcement, TLS refusal) and `smoke/replication_blackbox.sh` 19/19 (replica re-pointed
between masters, promotion).

The repo's e2e command-surface suite (`tests/e2e`, the `command-surface` job) ran in the release run on
amd64 for RESP2 and RESP3; we ran it again on **arm64** against the published image: **1,171 passed,
65 skipped, 0 failed** for both RESP2 and RESP3 (`results/arm64/e2e-resp*.log`). The two tests the
workflow still deselects (`test_acid.py` balance transfer and multi-key transfer) are deselected here too,
to match it; the workflow's own comment says they are fixed and the lines can go.

## Results

| Artifact | Architecture | Kind | Variant | Checks passed | Failed | Result |
|---|---|---|---|---|---|---|
| `ghcr.io/kividbio/kividb:v1.2.0-rc4` | amd64 | container image | default | 59 | 0 | PASS |
| `ghcr.io/kividbio/kividb:v1.2.0-rc4-lua` | amd64 | container image | lua | 63 | 0 | PASS |
| `ghcr.io/kividbio/kividb:v1.2.0-rc4-tls` | amd64 | container image | tls | 62 | 0 | PASS |
| `ghcr.io/kividbio/kividb:v1.2.0-rc4-full` | amd64 | container image | full | 66 | 0 | PASS |
| `docker.io/kividb/kividb:v1.2.0-rc4` | amd64 | container image | default | 59 | 0 | PASS |
| `docker.io/kividb/kividb:v1.2.0-rc4-lua` | amd64 | container image | lua | 63 | 0 | PASS |
| `docker.io/kividb/kividb:v1.2.0-rc4-tls` | amd64 | container image | tls | 62 | 0 | PASS |
| `docker.io/kividb/kividb:v1.2.0-rc4-full` | amd64 | container image | full | 66 | 0 | PASS |
| `ghcr.io/kividbio/kividb:v1.2.0-rc4-amd64` | amd64 | container image | default | 59 | 0 | PASS |
| `ghcr.io/kividbio/kividb:v1.2.0-rc4-lua-amd64` | amd64 | container image | lua | 63 | 0 | PASS |
| `ghcr.io/kividbio/kividb:v1.2.0-rc4-tls-amd64` | amd64 | container image | tls | 62 | 0 | PASS |
| `ghcr.io/kividbio/kividb:v1.2.0-rc4-full-amd64` | amd64 | container image | full | 66 | 0 | PASS |
| `kividb-linux-x86_64.tar.gz` | amd64 | release tarball | default | 59 | 0 | PASS |
| `kividb-lua-linux-x86_64.tar.gz` | amd64 | release tarball | lua | 63 | 0 | PASS |
| `kividb-tls-linux-x86_64.tar.gz` | amd64 | release tarball | tls | 62 | 0 | PASS |
| `kividb-full-linux-x86_64.tar.gz` | amd64 | release tarball | full | 66 | 0 | PASS |
| `kividb_amd64.deb` | amd64 | .deb package (systemd) | default | 59 | 0 | PASS |
| `kividb.x86_64.rpm` | amd64 | .rpm package | default | 60 | 0 | PASS |
| `ghcr.io/kividbio/kividb:v1.2.0-rc4` | arm64 | container image | default | 59 | 0 | PASS |
| `ghcr.io/kividbio/kividb:v1.2.0-rc4-lua` | arm64 | container image | lua | 63 | 0 | PASS |
| `ghcr.io/kividbio/kividb:v1.2.0-rc4-tls` | arm64 | container image | tls | 62 | 0 | PASS |
| `ghcr.io/kividbio/kividb:v1.2.0-rc4-full` | arm64 | container image | full | 66 | 0 | PASS |
| `docker.io/kividb/kividb:v1.2.0-rc4` | arm64 | container image | default | 59 | 0 | PASS |
| `docker.io/kividb/kividb:v1.2.0-rc4-lua` | arm64 | container image | lua | 63 | 0 | PASS |
| `docker.io/kividb/kividb:v1.2.0-rc4-tls` | arm64 | container image | tls | 62 | 0 | PASS |
| `docker.io/kividb/kividb:v1.2.0-rc4-full` | arm64 | container image | full | 66 | 0 | PASS |
| `ghcr.io/kividbio/kividb:v1.2.0-rc4-arm64` | arm64 | container image | default | 59 | 0 | PASS |
| `ghcr.io/kividbio/kividb:v1.2.0-rc4-lua-arm64` | arm64 | container image | lua | 63 | 0 | PASS |
| `ghcr.io/kividbio/kividb:v1.2.0-rc4-tls-arm64` | arm64 | container image | tls | 62 | 0 | PASS |
| `ghcr.io/kividbio/kividb:v1.2.0-rc4-full-arm64` | arm64 | container image | full | 66 | 0 | PASS |
| `kividb-linux-aarch64.tar.gz` | arm64 | release tarball | default | 59 | 0 | PASS |
| `kividb-lua-linux-aarch64.tar.gz` | arm64 | release tarball | lua | 63 | 0 | PASS |
| `kividb-tls-linux-aarch64.tar.gz` | arm64 | release tarball | tls | 62 | 0 | PASS |
| `kividb-full-linux-aarch64.tar.gz` | arm64 | release tarball | full | 66 | 0 | PASS |
| `kividb_arm64.deb` | arm64 | .deb package (systemd) | default | 59 | 0 | PASS |
| `kividb.aarch64.rpm` | arm64 | .rpm package | default | 60 | 0 | PASS |

### Finding: the `.rpm` needs `shadow-utils` but does not say so

The package's `%pre` script runs `groupadd`/`useradd` to create the `kividb` user. The package declares
no `Requires(pre): shadow-utils`, so on an image without it (the official `amazonlinux:2023` container,
UBI-minimal and similar) `dnf install` fails with `groupadd: command not found` / `Error in PREIN
scriptlet`. Every full Amazon Linux 2023 or RHEL host has `shadow-utils`, so a normal host install works;
the harness installs `shadow-utils` first (as a full host has it) and then the package passes all 60
checks on both architectures. Fix: declare `shadow-utils` as a dependency of the rpm in
`packaging/nfpm.yaml` (`overrides: rpm: depends: [shadow-utils]`). The `.deb` is not affected
(`passwd` is an essential package on Debian/Ubuntu).

## Also: one pass on `release/1.2.0` at 93524a1b (not rc4)

`release/1.2.0` at **93524a1b** carries the fixes found in this night's runs: the replica AOF backlog,
treating a zero-filled AOF tail as truncated after a power cut, and the cluster address fix. It was built
from source on an Azure D4s_v5 (`cargo build --release`, and again with `--features lua,tls`) and put
through the same per-artifact checks as a binary: **default variant 59/59, `lua,tls` build 66/66**
(`results/release-1.2.0-93524a1b/`). Version string `1.2.0`. No images or packages exist for that commit,
so those were not re-tested.

## Hardware and software

* Region **Central India** (the assigned region, Southeast Asia, offers no D-series VM to this
  subscription: `NotAvailableForSubscription` for every size).
* amd64: `ops-smx`, Azure `Standard_D4s_v5` (4 vCPU Intel Xeon Platinum 8573C, 16 GB).
  arm64: `ops-sma`, `Standard_D4ps_v5` (4 vCPU Ampere Altra / Neoverse-N1, 16 GB).
* Ubuntu 24.04.5 LTS, kernel 7.0.0-1017-azure, Docker 29.1.3, redis-cli 7.0 (Ubuntu's `redis-tools`),
  ORAS 1.2.2. RPM test container: `amazonlinux:2023`.

## Reproduce

On an Ubuntu 24.04 amd64 host and on an arm64 host, with Docker, `redis-tools`, `openssl`, `jq` and ORAS:

```bash
git clone https://github.com/kividbio/kividb src && git -C src checkout v1.2.0-rc4
bash harness/smoke_all.sh v1.2.0-rc4 "$PWD/src"        # writes ~/smoke-results/<arch>/
bash harness/e2e_published.sh v1.2.0-rc4 "$PWD/src" ~/smoke-results/<arch>   # optional, ~3 min
python3 harness/smoke_table.py ~/smoke-results/*/summary.tsv
```

One artifact on its own: `bash harness/smoke_artifact.sh image ghcr.io/kividbio/kividb:v1.2.0-rc4-full full t1`
(kinds: `image`, `bin <path/to/kividb>`, `deb <file>`, `rpm <file>`). Logs per artifact are in
`results/<arch>/<artifact>.log`, one `PASS:`/`FAIL:` line per check.
