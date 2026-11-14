# 1.3.0 regression suite

Every performance, latency or memory improvement in 1.3.0 gets a suite here: a
re-runnable harness, the numbers it recorded, and a gate that fails when a
later build loses the improvement. Each suite measures three engines on the
same machine, one server at a time:

- **v1.2.2**: the release tarball from GitHub, checked against the release's
  `SHA256SUMS.txt`. It never changes, so "how much better than 1.2.2" can be
  measured again on any machine, in the same session.
- **candidate**: the build under test (a binary, or a release tag).
- **Redis 8.10.x** (8.10.2 by default), for the gates that compare with Redis.

Suites 01-13 cover every measured gain in the "What 1.3.0 changed" table of
`docs/PERFORMANCE.md`; see [Not covered](#not-covered) for what is left out
and why.

| suite | issue / fix | what it measures |
|---|---|---|
| `01-scan-match` | #751 / #769 | full `SCAN` iterations over 2,002,000 keys (`MATCH schedule:*`, no `MATCH`, `*:1*`, `COUNT 10000`), `KEYS schedule:*`; `HSCAN`/`SSCAN`/`ZSCAN` of 200k-element collections as guard rows for #770 |
| `02-growth-ping` | #752 / #780 | worst `PING` while 8M keys are loaded in 2,000-`SET` pipelines; Tokio, and io_uring with `--threads 1`; `FULL=1` adds 17.5M keys with `EX` |
| `03-give-back` | #753 / #763 | RSS 1.5, 5 and 15 s after deleting a 3M-field hash, `HDEL` of 1M fields, and a mixed churn |
| `04-long-commands` | #754 / #757 | `KEYS` over 2M keys, `SCAN COUNT 2000000`, `DEL` of a 3M-field hash, each with 4 `PING` probes; Tokio and io_uring |
| `05-stream-memory` | #639 / #683 (#713) | `used_memory` per stream entry: 1M one-field entries (and RSS), 200k entries of five shared fields, five unique fields, one 200-byte value; `XADD`/`XRANGE` rates as cost guards |
| `06-stream-consumers` | #637 / #736 | 200k pending over 50 consumers: `XINFO CONSUMERS`, `XPENDING` summary and per consumer, `XREADGROUP` history; `XADD` rate while `XINFO CONSUMERS` is polled every 50 ms; bytes per pending entry |
| `07-save-ping` | #605 / #696 | worst `PING` during a `SAVE` of 1.5M keys, Tokio `--threads 1` |
| `08-ft-info` | #590 / #727 | `FT.INFO` of 1,000-document indexes (TEXT, vectors alone) beside 5.4M keys; `SET` rate beside 4 `FT.INFO` pollers; `HSET` and bytes per document of a vector-only index as cost guards |
| `09-vector-sets` | #697 / #730 | runs `../vector-sets/bench.py`: bytes per vector and recall@10 of 50,000 768-d vectors in `Q8` and `BIN`; `VSIM` q/s of 10,000 `Q8` vectors from 8 clients |
| `10-redisvl` | #767 / #779 | the RedisVL 0.32.0 harness of `benchmarks/redisvl-compat/` (67 checks), in its own venv |
| `11-migrate-conns` | #726 / #733 | connections the target of 2,000 `MIGRATE`s receives (a second server of the engine) |
| `12-search-fanout` | #723 / #725 | 3-master cluster on one VM: connections each other master receives for 2,000 `FT.SEARCH` calls |
| `13-cluster-reshard` | #294 / #724 | 3-master cluster on one VM, 3M keys: `redis-cli` 7.0.15 `--cluster reshard` of 1,000 slots |

## Running it

Linux only (it reads `/proc/<pid>/status`, and io_uring needs a kernel and a
sandbox that allow it: not Docker's default seccomp profile). Run it on a VM,
not on a laptop, and nothing else on the machine.

```sh
cargo build --release
benchmarks/1.3.0/regressions/run-all.sh --candidate target/release/kividb \
    --candidate-commit "$(git rev-parse --short HEAD)" --label master-$(git rev-parse --short HEAD)-d4s_v5
```

| option | |
|---|---|
| `--candidate <path\|tag>` | a kividb binary, or a release tag such as `v1.3.0` (its default-build tarball, SHA-256 checked) |
| `--label NAME` | results go to `results/NAME/` (default `run-<UTC time>`) |
| `--only 01,04` | only these suites (number or full name); an entry that names no suite is an error |
| `--engines v1_2_2,candidate` | only these engines; gates that need a missing engine are reported as skipped |
| `--record` | write this run's values into `baseline.json` (see below) |
| `--candidate-commit SHA` | recorded with the values (default: the binary's `--version`) |
| `FULL=1` | also the long configurations (`02-growth-ping/ex17m`: 17.5M keys, about 8 more minutes) |
| `SCALE=0.01` | a smoke run of the code paths with tiny datasets: verdict `SMOKE`, gates not checked, `--record` refused |
| `ALLOW_VM_CHANGE=1` | compare (or record) on another VM size than the one the baseline was recorded on |
| `REGRESS_REDIS_SHA256=<sha>` | accept a redis-server other than the pinned one (a deliberate Redis bump; `--record` then pins it) |
| `REDIS_SERVER`, `REDIS_VERSION` | a Redis binary to use; otherwise `/opt/redis-$REDIS_VERSION/src/redis-server`, otherwise Redis is built from its tag |
| `REGRESS_CACHE` | downloads, the Redis build and the Python venv (default `~/.cache/kividb-regress`) |
| `SUITE_TIMEOUT` | seconds one suite may take against one server (default 1800) |

`run-all.sh` creates a venv with the client pinned in `requirements.txt`
(redis-py 5.2.1), downloads and checks v1.2.2 (the default build, used by these
suites, and the `full` build, for suites that need Lua), starts each server on
a free port below the ephemeral range, waits for `PING`, runs the suite,
stops the server (`SHUTDOWN NOSAVE`, then a signal), and always stops it on
exit. Then `lib/compare.py` compares the run with `baseline.json`:

- `results/<label>/summary.md` and `summary.json`: every metric for the three
  engines, the recorded value, each gate's limit and result;
- `results/<label>/env.json`: VM size, CPU, kernel, glibc, Python, redis-py,
  each binary's path, version and SHA-256, the candidate's allocator, `FULL`,
  `SCALE`, `--only`, the engines, the suite/configurations that were run, any
  failures, and the run time of each suite;
- `results/<label>/raw.tar.gz`: per server run, the metrics with their
  samples, the slowest `PING`s, RSS timelines and the server's log tail.

The verdict and exit status:

| verdict | when | exit |
|---|---|---|
| `ok` | every gate passed | 0 |
| `REGRESSED` | a gate failed | 1 |
| `INVALID` | the run cannot be judged: a suite failed to import, list its configurations or finish, a server did not start, nothing ran, a recorded metric of a suite/configuration that ran is missing or lacks an engine's value, the VM size differs from the recorded one, or the v1.2.2 or redis-server binary differs from the SHA-256 pinned in `baseline.json` (`pins`) | 1 |
| `SMOKE` | `SCALE` is not 1: only that every metric came out | 0 |

A wrong argument (an unknown `--only` entry or engine) exits 2 before
anything runs. `--record` refuses (exit 1, `baseline.json` untouched) on an
`INVALID` run, a `SCALE` run, a run without all three engines, or a null
value, and never overwrites a recorded value with null.

Binaries: the v1.2.2 tarball (cached or not) is checked against the release's
`SHA256SUMS.txt` on every run and the binary unpacked from it afresh; its
SHA-256 must also equal `pins.v1_2_2_sha256`
(`38fc2cea40c131e041f4914527a98719e5d34dffdfe2abdc1f07ecb86aa6723e`).
redis-server must match `pins.redis_sha256`
(`b0cd304980112fa6cd8dced524fdd6b136fd8efb802f5cf9e81610051a11891c`, the
8.10.2 build in `/opt/redis-8.10.2` of our Azure VM image; a build made
elsewhere differs, so set `REGRESS_REDIS_SHA256` to use it). Suite 03 measures
glibc malloc's give-back: a candidate that links mimalloc (a `lua` or `full`
build) fails it on purpose.

Suites 05-13 also use `redis-benchmark` and `redis-cli` from the directory of
the Redis binary, numpy (pinned in `requirements.txt`, for
`../vector-sets/bench.py`), a second venv for RedisVL (`10-redisvl`,
`$REGRESS_CACHE/venv-redisvl`, from `../../redisvl-compat/requirements.txt`)
and a `redis-cli` 7.0.x for `13-cluster-reshard` (`$REDIS_CLI_7`, else
`/usr/bin/redis-cli` if it is 7.0.x, as Ubuntu 24.04's `redis-tools` is, else
built from Redis's 7.0.15 tag into `$REGRESS_CACHE`, which needs git, make
and a C compiler).

## Method

- One server at a time, started fresh for each configuration, in an empty
  directory, with no persistence (`--save ''`/`--appendonly no` for Redis;
  KiviDB saves nothing by default). KiviDB runs with `--protected-mode no
  --bind 127.0.0.1`, `--io-model` and `--threads` as the configuration says
  (`auto` and the default thread count otherwise). Redis runs with its
  defaults (`io-threads 1`, jemalloc) and `--enable-debug-command yes`.
- Client and server share the machine. Absolute numbers depend on it; the
  gates that compare with v1.2.2 or Redis measure both in the same session,
  and the gates against recorded values are only meaningful on the VM size
  they were recorded on (`recorded.vm`).
- Fixed data: the datasets are generated in order, and the churn scenario uses
  `random.Random(7)`.
- Warm-ups and medians: SCAN cases run one warm-up iteration and three timed
  ones (median); long commands run three times (median, and the max for the
  Tokio bound). Loads (suite 02) and give-back scenarios (suite 03) run once per
  server: each one is the measurement, and takes 15-140 s.
- PING probes (`lib/probe.py`) are separate processes with raw sockets,
  each sending `PING` every 1 ms, so they share no GIL with the workload. The
  issues' harnesses ran the probe as a thread of the loader, which inflates the
  probe's p99 (and Redis's worst PING in #752): compare numbers within this
  suite, not with the issues.
- RSS is `VmRSS` from `/proc/<pid>/status`; after a delete the harness sends
  nothing until the 15 s mark, because the give-back thread waits for an idle
  server.
- Memory per item (streams, pending entries, vector-set elements, index
  documents) is the growth of `INFO used_memory` over the load divided by the
  items, as the PRs measured it; it is deterministic to the byte for a given
  build.
- Throughput rows that the PRs took with `redis-benchmark` use it here too
  (`-c 4 -P 16` for the stream cost rows, `-c 20` for `XADD` beside polling,
  `-c 8` for `SET`/`HSET` beside `FT.INFO`), median of 3 where it is cheap.
  Beside the `FT.INFO` pollers it runs for a fixed 10 s, not a fixed count,
  because v1.2.2 gets through only 30-115 `SET`s a second there
  (`lib/nodes.py` `redis_benchmark_for`).
- Several servers on one VM (`11-migrate-conns`, `12-search-fanout`,
  `13-cluster-reshard`): a configuration declared `self_hosted` gets no server
  from `run-all.sh`; the suite starts its own from the engine's binary
  (`lib/nodes.py`), each in an empty directory on its own port (cluster nodes
  also keep the bus port, port + 10000, below the ephemeral range), forms the
  a node that fails to start has its directory and log removed before the error is raised, and every node's pid
  goes to a run-scoped directory (`$REGRESS_PIDDIR`) so that `run-all.sh` kills nodes a SIGKILLed suite left
  behind, only pids whose `/proc/<pid>/cmdline` still holds the recorded binary; `ctx.client()` on such a
  configuration raises (use `node.client()`),
  cluster with `CLUSTER ADDSLOTSRANGE` (slots split evenly) and `CLUSTER MEET`,
  waits for `cluster_state:ok` on every node, and stops every node on exit,
  on an error and on `SIGTERM` (the suite timeout). The nodes share the VM's 4
  vCPUs, as the PRs' nodes did (#724 and #725 ran theirs on one D4s_v5 too).
- KiviDB-only rows: the Redis 8.10.2 of the VM image has no Query Engine or
  JSON module, so `08-ft-info`, `10-redisvl` and `12-search-fanout` run v1.2.2
  and the candidate only (none of their PRs measured Redis either, except
  RedisVL's 67/67 on the `redis:8.10` image).

## Tolerances (gates)

Each metric in `baseline.json` has one gate or a list of them; all must pass.
They are checked against the candidate only.

| kind | rule | used for |
|---|---|---|
| `rel` | within `pct` % of the recorded candidate value, in the worse direction; `abs` adds a slack in the metric's unit (the larger wins); `redis_plus` also allows up to Redis's value this session plus that much | throughput/time: 10 %; memory held: 5 %; RSS after a give-back: 25 % or 5 MiB; worst PING: max(1.5x recorded, Redis + 10 ms); #770 guard rows: 35 % (measured spread up to 28 %) |
| `ratio` | the gain over v1.2.2 measured this session (v1.2.2/candidate for lower-is-better) is at least `keep` x the recorded gain | every claimed improvement: keep 60 % |
| `vs_redis` | candidate at most `max_x` times worse than Redis this session | SCAN iterations: 1.3x |
| `bound` | absolute: `eq` (`"recorded"`: equal to the recorded value), `max`, `min` | counts (exact); worst PING on Tokio during a long command: 20 ms |
| `info` | shown, never gated | call counts, PINGs over 10 ms, probes stalled |

Why a few rows differ from the plan's defaults:

- `04-long-commands` on Tokio: the worst PING is 0.4-8 ms against 130-260 ms
  on v1.2.2. A ratio of those two is mostly noise in the denominator, and
  "Redis + 10 ms" would allow the full stall back (Redis stalls everyone). The
  gate is an absolute 20 ms, on the median and on the worst of the 3 runs.
- `04-long-commands` on io_uring is not fixed yet (#762): the probes that share
  the command's thread still wait, and how many of the four do is chance. The
  worst PING over all probes is shown, not gated; the gate is on the probe that
  waited least (`*_least_worst_ping_ms`, 1.5x or +10 ms), which fails if a long
  command holds up every connection. The fix will show up as an improvement.
- `01-scan-match` `HSCAN`/`SSCAN`/`ZSCAN` (#770) are 10-20x slower than Redis
  today: gated only against getting slower than recorded.
- Memory per item (`05`, `06`, `08`, `09`) is deterministic, so 5 % is wide;
  a few bytes of `abs` slack (1-4 B) keep the small values (6.2 B per one-field
  stream entry) from failing on an allocator rounding.
- `06`'s `xinfo_polls` (XINFO CONSUMERS calls during the second XADD run) is gated `min` 5 (recorded 42, v1.2.2 14);
  a poller thread that raises fails the suite instead of leaving the XADD rate unpolled.
- Connection counts (`11`, `12`) and the worst `PING` during a `SAVE` (`07`)
  are absolute bounds, from the PRs' own tests: at most 3 connections for
  2,000 `MIGRATE`s, at most 20 per other master for 2,000 searches (16 kept +
  4), at most 20 ms. A ratio against 1 connection or 1 ms means nothing.
- `XADD` rates beside polling (`06`) move by up to 14 % between runs: 25 %.
  The `SET` rate beside `FT.INFO` pollers (`08`) has no ratio gate: v1.2.2's
  rate (30-115 a second) is too noisy a denominator.
- `SAVE` time (`07`) and `VADD` rates of the 50k memory rows (`09`) are shown,
  not gated (the same build's `SAVE` took 339 and 503 ms in two runs).
- `09-vector-sets` `VSIM` q/s: rel 25 % (#730 saw 15 % between repeats) and no
  ratio, because the Q8 search gain does not show on this VM (see below).

## Baselines

`baseline.json` has one entry per metric, keyed `suite/config/metric`:

```json
"02-growth-ping/tokio/ping_max_ms": {
  "suite": "02-growth-ping", "config": "tokio", "metric": "ping_max_ms",
  "unit": "ms", "better": "lower",
  "recorded": {"v1_2_2": 112.0991, "candidate": 9.6577, "redis": 8.1904,
               "vm": "Standard_D4s_v5", "date": "2026-11-14", "commit": "2468f226",
               "label": "master-2468f226-d4s_v5"},
  "gate": [{"kind": "rel", "pct": 50, "redis_plus": 10}, {"kind": "ratio", "keep": 0.6}]
}
```

To record new values, run with `--record` (on the VM size the baseline names,
from a clean tree), check `summary.md`, then commit `baseline.json` and
`results/<label>/`. A re-record keeps each metric's gate, so a tolerance tuned
by hand survives; `lib/compare.py <dir> --record --reset-gates` takes the
suites' declared gates instead. Only record values you have explained: a
re-record that hides a regression defeats the suite.

## How a release uses it

1. Once the release's scope is frozen and the final RC is cut (never on a
   build that may still change), run the whole suite with `FULL=1` against the
   RC on the VM size in `baseline.json` (`Standard_D4s_v5`):
   `run-all.sh --candidate v1.3.0-rcN --label v1.3.0-rcN-d4s_v5`.
2. Every gate must pass. A failure is either a regression to fix before the
   release, or an intended change that is explained in the release PR, after
   which the baseline is re-recorded.
3. Commit `results/<label>/` with the release PR, so the release's numbers
   stay next to the harness that produced them. The release workflow
   refuses a stable tag without it (`scripts/release/check-regression-results.sh`:
   a full run with verdict `ok` whose candidate commit is an ancestor of the tag,
   with `src/`, `Cargo.toml` and `Cargo.lock` unchanged since).
   The Actions workflow "Regression suite" (`.github/workflows/regression-suite.yml`)
   makes the VM, builds the candidate, runs the suite and uploads
   `results/<label>/` as an artifact; docs/RELEASING.md has the steps.
4. For a new improvement in the release, its suite and its first recorded
   values land with the fix's PR (or right after it), not at release time.

## Adding a suite

A suite is `suites/NN-name.py`. `run-all.sh` picks up every file named
`[0-9][0-9]-*.py`, in order.

```python
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib"))
from suite import Suite, Config, THROUGHPUT, MEMORY, WORST_PING, KEEP_IMPROVEMENT, EXACT, rel, bound, info, median, scaled
from probe import Probe   # PING probes
from rss import vmrss_mib, used_memory_mib, sample_at

S = Suite("05-example", [
    Config("tokio", io="tokio"),                                  # all three engines
    Config("uring1", io="uring", threads=1, engines=("v1_2_2", "candidate")),
    Config("big", full_only=True),                               # FULL=1 only
])

@S.run
def run(ctx):                 # one engine, one configuration, a fresh server
    c = ctx.client()          # redis-py client on ctx.port; ctx.pid is the server's pid
    ...                       # ctx.engine: v1_2_2 | candidate | redis; ctx.kind: kividb | redis
    ctx.emit("load_s", secs, "s", "lower", gate=THROUGHPUT, samples=[...])
    ctx.raw["anything"] = ...  # goes into raw.tar.gz

if __name__ == "__main__":
    sys.exit(S.main())
```

- Use the named tolerances in `lib/suite.py` (`THROUGHPUT`, `MEMORY`,
  `GIVE_BACK`, `WORST_PING`, `KEEP_IMPROVEMENT`, `EXACT`) unless the metric
  needs another, and say why in the suite's docstring and in this README.
- Make the data deterministic, warm up, take the median of 3 where a run is
  cheap, and keep each server run under `SUITE_TIMEOUT`.
- Wrap dataset sizes in `scaled()` so `SCALE=0.01` can smoke-test the suite.
- Suites that need Lua use the `full` build: the v1.2.2 one is fetched to
  `$REGRESS_CACHE/releases/v1.2.2/full/kividb` (extend `run-all.sh`'s engine
  table with a `v1_2_2_full`/`candidate_full` pair when the first such suite
  lands).
- Smoke it (`SCALE=0.01 run-all.sh --only 05 ...`), then run it for real with
  `--only 05 --record`, and twice more without `--record` to see the noise;
  widen a tolerance only for noise you measured.
- A suite that needs several servers declares its configuration
  `Config(..., self_hosted=True)` and starts them with `lib/nodes.py`
  (`with Nodes(ctx) as ns: nodes = ns.cluster(3)`); `ctx.bin` is the engine's
  binary and `ctx.port`/`ctx.pid` are None. A suite that needs one more
  server beside run-all.sh's uses `ns.start()` the same way.
- Add its row to the table at the top and its numbers below.

## Recorded run, suites 01-04

Recorded 2026-11-14 on an Azure Standard_D4s_v5 (4 vCPU, Xeon Platinum 8370C,
16 GB, Ubuntu 24.04, kernel 7.0.0-1017-azure, glibc 2.39), Python 3.12.3,
redis-py 5.2.1. Client and server on the same VM.

| engine | build | SHA-256 |
|---|---|---|
| v1.2.2 | `kividb-linux-x86_64.tar.gz` of the v1.2.2 release (checked against its `SHA256SUMS.txt`) | tarball `3af9821b71801006835b7fc208a16a890fb0c8fa709ffabf2ad115641da9c51d`, binary `38fc2cea40c131e041f4914527a98719e5d34dffdfe2abdc1f07ecb86aa6723e` |
| candidate | master `2468f226`, `cargo build --release` (LTO fat, default features: glibc, no Lua) on the VM | binary `76dcdd0414c70f4501de77d99f9dcf189c2b4d5f626ac6c0fc12843aa9d6e7af` |
| Redis | 8.10.2 built from its tag (`/opt/redis-8.10.2` on our VM image), jemalloc 5.3.0 | binary `b0cd304980112fa6cd8dced524fdd6b136fd8efb802f5cf9e81610051a11891c` |

Runs, all in `results/`:

- `master-2468f226-d4s_v5`: the recorded run, `FULL=1 --record`, all four suites.
- `master-2468f226-d4s_v5-04`: suite 04 recorded again (its summary says
  `rec04`) after its io_uring gate moved from the median probe to the
  least-stalled probe (the median of four probes flips when two of them share
  the command's ring).
- `master-2468f226-d4s_v5-rerun`: the noise check, the same binaries again
  without `FULL`, compared with the baseline: every gate passed.

Run time per suite: 01 136 s, 02 261 s (711 s with `FULL=1`), 03 200 s,
04 200 s; about 13 minutes for a default run, 21 with `FULL=1`.

Recorded values (medians where the suite takes them; counts, call counts and
informational rows are in `baseline.json` and the summaries):

| suite / config | metric | v1.2.2 | candidate | Redis 8.10.2 | gate |
|---|---|---:|---:|---:|---|
| 01-scan-match / default | hscan_match_c1000_ms (ms) | 443.7 | 428.0 | 41.4 | rel 35% |
| 01-scan-match / default | keys_schedule_ms (ms) | 116.9 | 60.4 | 130.6 | rel 10% |
| 01-scan-match / default | scan_all_c1000_ms (ms) | 3,616 | 2,180 | 2,213 | rel 10%; keep 60% of gain; <= 1.3x Redis |
| 01-scan-match / default | scan_infix_c1000_ms (ms) | 2,696 | 1,494 | 1,801 | rel 10%; keep 60% of gain; <= 1.3x Redis |
| 01-scan-match / default | scan_match_c10000_ms (ms) | 823.3 | 238.9 | 390.7 | rel 10%; keep 60% of gain; <= 1.3x Redis |
| 01-scan-match / default | scan_match_c1000_ms (ms) | 1,672 | 406.2 | 570.5 | rel 10%; keep 60% of gain; <= 1.3x Redis |
| 01-scan-match / default | sscan_match_c1000_ms (ms) | 943.5 | 1,093 | 38.9 | rel 35% |
| 01-scan-match / default | zscan_match_c1000_ms (ms) | 799.5 | 829.5 | 50.4 | rel 35% |
| 02-growth-ping / ex17m | load_s (s) | 145.5 | 143.2 | 149.1 | rel 10% |
| 02-growth-ping / ex17m | ping_max_ms (ms) | 160.5 | 17.0 | 12.2 | rel 50% or Redis+10 |
| 02-growth-ping / ex17m | ping_p99_ms (ms) | 3.36 | 3.64 | 1.91 | rel 50% or Redis+10 |
| 02-growth-ping / ex17m | rss_loaded_mib (MiB) | 1,506 | 1,507 | 2,084 | rel 5% |
| 02-growth-ping / ex17m | used_memory_mib (MiB) | 1,613 | 1,613 | 2,027 | rel 5% |
| 02-growth-ping / tokio | load_s (s) | 51.3 | 50.8 | 52.2 | rel 10% |
| 02-growth-ping / tokio | ping_max_ms (ms) | 112.1 | 9.66 | 8.19 | rel 50% or Redis+10; keep 60% of gain |
| 02-growth-ping / tokio | ping_p99_ms (ms) | 1.98 | 3.03 | 1.38 | rel 50% or Redis+10 |
| 02-growth-ping / tokio | rss_loaded_mib (MiB) | 524.2 | 525.0 | 552.0 | rel 5% |
| 02-growth-ping / tokio | used_memory_mib (MiB) | 511.4 | 511.4 | 534.7 | rel 5% |
| 02-growth-ping / uring1 | load_s (s) | 53.3 | 55.6 | - | rel 10% |
| 02-growth-ping / uring1 | ping_max_ms (ms) | 72.7 | 20.8 | - | rel 50% or Redis+10; keep 60% of gain |
| 02-growth-ping / uring1 | ping_p99_ms (ms) | 0.71 | 1.54 | - | rel 50% or Redis+10 |
| 02-growth-ping / uring1 | rss_loaded_mib (MiB) | 524.4 | 524.5 | - | rel 5% |
| 02-growth-ping / uring1 | used_memory_mib (MiB) | 511.5 | 511.6 | - | rel 5% |
| 03-give-back / churn | rss_after_15s_mib (MiB) | 483.5 | 429.4 | 616.5 | rel 25% or +5 |
| 03-give-back / churn | rss_after_1_5s_mib (MiB) | 483.5 | 429.4 | 737.1 | rel 25% or +5 |
| 03-give-back / churn | rss_after_5s_mib (MiB) | 483.5 | 429.4 | 670.2 | rel 25% or +5 |
| 03-give-back / churn | rss_loaded_mib (MiB) | 788.6 | 789.3 | 746.3 | rel 5% |
| 03-give-back / churn | used_loaded_mib (MiB) | 742.5 | 742.6 | 719.1 | rel 5% |
| 03-give-back / hashdel | rss_after_15s_mib (MiB) | 196.4 | 13.6 | 19.5 | rel 25% or +5 |
| 03-give-back / hashdel | rss_after_1_5s_mib (MiB) | 196.4 | 13.6 | 75.8 | rel 25% or +5; keep 60% of gain |
| 03-give-back / hashdel | rss_after_5s_mib (MiB) | 196.4 | 13.6 | 56.2 | rel 25% or +5 |
| 03-give-back / hashdel | rss_loaded_mib (MiB) | 369.6 | 369.9 | 124.5 | rel 5% |
| 03-give-back / hashdel | used_loaded_mib (MiB) | 295.9 | 295.9 | 109.2 | rel 5% |
| 03-give-back / hdel | rss_after_15s_mib (MiB) | 74.1 | 13.6 | 13.6 | rel 25% or +5 |
| 03-give-back / hdel | rss_after_1_5s_mib (MiB) | 74.1 | 13.6 | 39.3 | rel 25% or +5; keep 60% of gain |
| 03-give-back / hdel | rss_after_5s_mib (MiB) | 74.1 | 13.6 | 24.5 | rel 25% or +5 |
| 03-give-back / hdel | rss_loaded_mib (MiB) | 137.7 | 138.2 | 47.3 | rel 5% |
| 03-give-back / hdel | used_loaded_mib (MiB) | 132.8 | 132.8 | 29.5 | rel 5% |
| 04-long-commands / tokio | del_cmd_ms (ms) | 132.2 | 143.3 | 333.5 | rel 10% |
| 04-long-commands / tokio | del_worst_ping_max_ms (ms) | 131.2 | 1.22 | 335.7 | max 20 |
| 04-long-commands / tokio | del_worst_ping_ms (ms) | 131.0 | 0.94 | 332.7 | max 20 |
| 04-long-commands / tokio | keys_cmd_ms (ms) | 141.1 | 161.3 | 216.5 | rel 10% |
| 04-long-commands / tokio | keys_worst_ping_max_ms (ms) | 132.1 | 8.09 | 205.1 | max 20 |
| 04-long-commands / tokio | keys_worst_ping_ms (ms) | 129.3 | 5.94 | 203.6 | max 20 |
| 04-long-commands / tokio | scan_cmd_ms (ms) | 254.3 | 112.5 | 471.7 | rel 10% |
| 04-long-commands / tokio | scan_worst_ping_max_ms (ms) | 243.4 | 5.19 | 467.2 | max 20 |
| 04-long-commands / tokio | scan_worst_ping_ms (ms) | 241.0 | 0.45 | 461.2 | max 20 |
| 04-long-commands / uring | del_cmd_ms (ms) | 136.4 | 134.3 | - | rel 10% |
| 04-long-commands / uring | del_least_worst_ping_ms (ms) | 0.21 | 0.41 | - | rel 50% or +10 |
| 04-long-commands / uring | del_worst_ping_ms (ms) | 135.8 | 132.9 | - | info |
| 04-long-commands / uring | keys_cmd_ms (ms) | 145.6 | 153.6 | - | rel 10% |
| 04-long-commands / uring | keys_least_worst_ping_ms (ms) | 0.17 | 0.95 | - | rel 50% or +10 |
| 04-long-commands / uring | keys_worst_ping_ms (ms) | 134.9 | 142.8 | - | info |
| 04-long-commands / uring | scan_cmd_ms (ms) | 265.5 | 112.6 | - | rel 10% |
| 04-long-commands / uring | scan_least_worst_ping_ms (ms) | 0.57 | 0.13 | - | rel 50% or +10 |
| 04-long-commands / uring | scan_worst_ping_ms (ms) | 254.9 | 101.6 | - | info |

Run-to-run spread of the candidate between the recorded run and the rerun:
SCAN iterations -2 to +6 %, `KEYS` -2 %, loads -1 to -8 %, memory 0 to +0.2 %,
RSS after give-back +0.7 to +2.2 % (0.1-0.3 MiB), long-command times -3 to
+4 %. Larger: `SSCAN`/`ZSCAN` of 200k members -28 % / -16 % (hence their 35 %
gate), the worst PING of the growth load (9.7 vs 8.5 ms on Tokio, 20.8 vs
9.4 ms on io_uring with one thread), and sub-10 ms worst PINGs during long
commands on Tokio (0.4-8 ms), which is why those use an absolute 20 ms bound.

What this run shows besides the gated gains:

- `KEYS user:99*` over 2M keys takes 161-164 ms on master against 141-142 ms
  on v1.2.2 on Tokio (about 15 % slower), and 154-163 ms against 146-149 ms on
  io_uring; `KEYS schedule:*` is twice
  as fast. The gate holds today's value.
- The incremental table growth (#780) raised the growth load's PING p99 on
  Tokio from 2.0 to 3.0 ms (and 0.7 to 1.5 ms on io_uring with one thread),
  while its worst PING fell from 112 to 9.7 ms.
- `SCAN 0 MATCH user:99* COUNT 2000000` takes 112 ms instead of 254-265 ms.

## Recorded run, suites 05-13

Recorded 2026-11-14 on the same VM size and image as suites 01-04 (Azure
Standard_D4s_v5, Xeon Platinum 8370C, Ubuntu 24.04, kernel 7.0.0-1017-azure,
glibc 2.39, Python 3.12.3, redis-py 5.2.1, numpy 2.1.3), a fresh VM, with the
same v1.2.2 and Redis binaries (the pinned SHA-256s). The candidate is master
`d66f0f93` (`cargo build --release`, default build, glibc), binary
`9771dc1ec052679fa29498cf39b3daedb3f5718d29bc1501732aa32c2df5f556`. `redis-cli`
for the reshard: Ubuntu's 7.0.15 (`/usr/bin/redis-cli`).

Runs, all in `results/`:

- `master-d66f0f93-d4s_v5-part2`: the recorded run, `--only 05,...,13 --record`.
- `master-d66f0f93-d4s_v5-part2-09`: suite 09 recorded again after the
  `f32_768_10k` configuration was added and its q/s gate lost the ratio.
- `master-d66f0f93-d4s_v5-part2-rerun1`: the first noise check. Its one
  failure, `07-save-ping` `save_ms` (503 ms against 339), is why the SAVE time
  is no longer gated; `06` and `08` gates were widened the same way, from
  this run's spread (see Tolerances).
- `master-d66f0f93-d4s_v5-part2-rerun2` and `master-d66f0f93-d4s_v5-part2-09-rerun`:
  the second noise check with the final gates: every gate passed.

Run time per suite: 05 109 s, 06 27 s, 07 10 s, 08 59 s, 09 180 s, 10 2 s
(after the RedisVL venv exists; its first install takes about a minute),
11 3 s, 12 4 s, 13 380-500 s (v1.2.2's reshard alone takes 270-380 s); about
13-15 minutes, so about 27 minutes for a default run of 01-13 and 35 with
`FULL=1`.

What each PR claimed and what this VM measured (the PRs' machines differ; the
gates compare within this suite):

| row of docs/PERFORMANCE.md | PR | PR: 1.2.2 / 1.3.0 / Redis | here: v1.2.2 / master / Redis 8.10.2 |
|---|---|---|---|
| `--cluster reshard` of 1,000 slots, 3 masters, 3M keys | #724 | 272.2 s / 23.6 s / - | 268-382 s / 25.3-27.0 s / 6.8-7.7 s (183,078 keys move) |
| connections each other master gets for 2,000 `FT.SEARCH` | #725 | 2,001 / <= 20 (1-16 kept) / n/a | 2,000 / 1 (2 kept) / n/a |
| connections the target gets for 2,000 `MIGRATE`s | #733 | 2,000 / 1 / 1 | 2,000 / 1 / 1 |
| stream, one field, 1M entries, B an entry | #683 | 158.4 / 6.1 / - | 158.4 / 6.23 / 13.1 |
| stream, five shared fields, 200k entries | #683 | 478.5 / 51.2 / - | 478.4 / 14.65 / 26.2 (values `v`, not the PR's) |
| stream, five unique field names | #683 | 498.6 / 101.1 / - | 478.4 / 54.0 / 75.4 (names `a<i>`..`e<i>`) |
| stream, one 200-byte value | #683 | 350.9 / 215.8 / - | 350.9 / 216.5 / 216.3 |
| RSS, 1M one-field entries | #683 | 207 MB / 22 MB / - | 195.8 / 20.7-20.9 / 25.4 MiB |
| `XINFO CONSUMERS`, 200k pending over 50 consumers | #736 | 82.5 / 0.059 ms / - | 65.0-65.8 / 0.37-0.40 / 0.36 ms (from Python, one client: includes the round trip) |
| `XADD` to a stream polled by `XINFO CONSUMERS` every 50 ms | #736 | 38,820 / 104,384 /s / - | 42,800-47,500 / 102,500-117,400 / 143,500-144,100 /s |
| another client's `PING` during a `SAVE` of 1.5M keys | #696 | 532 ms / at once / - | 292-336 / 1.1-2.0 / 501-588 ms (Redis blocks all clients by design) |
| `FT.INFO` beside 5.4M keys (p50) | #727 | 53 / 0.06 ms / - | 115-117 / 0.096-0.125 ms / - |
| `SET`s a second beside 4 `FT.INFO` pollers | #727 | 15-20k / 50-65k / - | 33-113 / 47,400-53,700 / - |
| vector set, 768-d, 50k, `Q8`, B a vector | #730 | 3,168 / 864 / 1,526 | 3,168 / 863.7 / 1,526 |
| the same, `BIN` | #730 | 3,168 / 192 / 854 | 3,168 / 191.7 / 853 |
| `VSIM` q/s, 768-d, 10k, `Q8`, 8 clients | #730 | 1,637 / 12,635 / 23,625 (HNSW) | 1,093-1,151 / 970-986 / 7,840-7,880 (HNSW) |
| RedisVL 0.32.0 checks passed | #779 | 59 / 67 / 67 | 59 / 67 / not run (no Query Engine module in this Redis build) |

Recorded values (counts, timings shown for information and the rows of
`load_s` are in `baseline.json` and the summaries):

| suite / config | metric | v1.2.2 | candidate | Redis 8.10.2 | gate |
|---|---|---:|---:|---:|---|
| 05-stream-memory / one1m | one_field_bytes_per_entry (B) | 158.4 | 6.23 | 13.10 | rel 5% or +1; keep 60% of gain |
| 05-stream-memory / one1m | one_field_rss_mib (MiB) | 195.8 | 20.71 | 25.44 | rel 5% or +5; keep 60% of gain |
| 05-stream-memory / shapes | shared5_bytes_per_entry (B) | 478.4 | 14.65 | 26.16 | rel 5% or +1; keep 60% of gain |
| 05-stream-memory / shapes | unique5_bytes_per_entry (B) | 478.4 | 54.01 | 75.36 | rel 5% or +1; keep 60% of gain |
| 05-stream-memory / shapes | val200_bytes_per_entry (B) | 350.9 | 216.5 | 216.3 | rel 5% or +1; keep 60% of gain |
| 05-stream-memory / shapes | xadd5_rps (req/s) | 332,447 | 535,332 | 516,529 | rel 15% |
| 05-stream-memory / shapes | xrange5_count10_rps (req/s) | 89,955 | 86,381 | 131,810 | rel 15% |
| 05-stream-memory / shapes | xrange_one_id_rps (req/s) | 485,437 | 443,262 | 454,545 | rel 15% |
| 06-stream-consumers / pel | pending_bytes_per_entry (B) | 115.9 | 122.3 | 166.7 | rel 5% or +2 |
| 06-stream-consumers / pel | xadd_other_stream_rps (req/s) | 103,520 | 104,384 | 139,958 | rel 25% |
| 06-stream-consumers / pel | xadd_polled_stream_rps (req/s) | 42,799 | 117,371 | 144,092 | rel 25%; keep 60% of gain |
| 06-stream-consumers / pel | xinfo_consumers_ms (ms) | 65.19 | 0.395 | 0.362 | rel 50% or +0.5; keep 60% of gain |
| 06-stream-consumers / pel | xpending_c50_ms (ms) | 1.61 | 0.11 | 0.0859 | rel 50% or +0.5; keep 60% of gain |
| 06-stream-consumers / pel | xpending_summary_ms (ms) | 7.59 | 0.193 | 0.165 | rel 50% or +0.5; keep 60% of gain |
| 06-stream-consumers / pel | xreadgroup_c50_history_ms (ms) | 1.59 | 0.133 | 0.0992 | rel 50% or +0.5; keep 60% of gain |
| 07-save-ping / tokio1 | ping_during_save_max_ms (ms) | 338.2 | 2.19 | 531.7 | max 20 |
| 07-save-ping / tokio1 | ping_during_save_ms (ms) | 333.3 | 1.48 | 501.4 | max 20 |
| 08-ft-info / ftinfo | ft_info_text_ms (ms) | 116.3 | 0.12 | - | rel 50% or +0.5; keep 60% of gain |
| 08-ft-info / ftinfo | ft_info_vec_ms (ms) | 116.7 | 0.125 | - | rel 50% or +0.5; keep 60% of gain |
| 08-ft-info / ftinfo | hset_vec_doc_rps (req/s) | 110,865 | 107,797 | - | rel 15% |
| 08-ft-info / ftinfo | set_rps_beside_ft_info (req/s) | 113.1 | 47,430 | - | rel 20% |
| 08-ft-info / ftinfo | vec_doc_bytes (B) | 474.5 | 494.1 | - | rel 5% or +4 |
| 09-vector-sets / bin_768_50k | bytes_per_vector (B) | 3,168 | 191.7 | 853.2 | rel 5%; keep 60% of gain |
| 09-vector-sets / bin_768_50k | recall_at_10 (ratio) | 1 | 0.103 | 0.107 | rel 2% |
| 09-vector-sets / f32_768_10k | recall_at_10 (ratio) | 1 | 1 | 1 | rel 2% |
| 09-vector-sets / f32_768_10k | vadd_per_s (VADD/s) | 66,395 | 62,038 | 2,350 | rel 25% |
| 09-vector-sets / f32_768_10k | vsim_qps (q/s) | 1,124 | 1,116 | 6,629 | rel 25% |
| 09-vector-sets / q8_768_10k | recall_at_10 (ratio) | 1 | 0.972 | 0.972 | rel 2% |
| 09-vector-sets / q8_768_10k | vadd_per_s (VADD/s) | 66,887 | 52,817 | 3,991 | rel 25% |
| 09-vector-sets / q8_768_10k | vsim_qps (q/s) | 1,109 | 986.1 | 7,877 | rel 25% |
| 09-vector-sets / q8_768_50k | bytes_per_vector (B) | 3,168 | 863.7 | 1,526 | rel 5%; keep 60% of gain |
| 09-vector-sets / q8_768_50k | recall_at_10 (ratio) | 1 | 0.965 | 0.963 | rel 2% |
| 10-redisvl / redisvl | checks_failed (checks) | 8 | 0 | - | max 0 |
| 10-redisvl / redisvl | checks_passed (checks) | 59.00 | 67.00 | - | exact |
| 11-migrate-conns / migrate | keys_left_on_source (keys) | 0 | 0 | 0 | exact |
| 11-migrate-conns / migrate | keys_on_target (keys) | 2,000 | 2,000 | 2,000 | exact |
| 11-migrate-conns / migrate | target_connections (connections) | 2,000 | 1 | 1 | max 3 |
| 12-search-fanout / cluster3 | peer_connections_max (connections) | 2,000 | 1 | - | max 20 |
| 12-search-fanout / cluster3 | searches_per_s (searches/s) | 4,520 | 5,772 | - | rel 25% |
| 12-search-fanout / cluster3 | searches_wrong_count (searches) | 0 | 0 | - | exact |
| 13-cluster-reshard / reshard1000 | cluster_check_failed (-) | 0 | 0 | 0 | max 0 |
| 13-cluster-reshard / reshard1000 | keys_after (keys) | 3,000,000 | 3,000,000 | 3,000,000 | exact |
| 13-cluster-reshard / reshard1000 | keys_moved (keys) | 183,078 | 183,078 | 183,078 | exact |
| 13-cluster-reshard / reshard1000 | reshard_s (s) | 381.8 | 25.30 | 7.11 | rel 15%; keep 60% of gain |

Run-to-run spread of the candidate over the three runs: memory per item 0 %
(deterministic), RSS +1 %; `redis-benchmark` rates of `05` -4 to +8 %, of
`06` -13 to +17 %, of `08` +1 to +13 %; command latencies of `06`/`08` 0 to
-22 % (sub-millisecond values from one Python client); worst `PING` during
`SAVE` 1.1-2.8 ms; `VSIM` q/s -3 to +18 % (the 50k rows: about 110-140 q/s)
and VADD/s 0 to +4 %; reshard +3 to +7 %; connection counts, key counts and
RedisVL checks exact.

What these runs show besides the gated gains:

- **The `Q8` search speed-up of #730 does not reproduce on this x86-64 VM.**
  Stored as `Q8`, 10,000 768-d vectors answer `VSIM` at 970-986 q/s, against
  1,081-1,116 q/s for the same build storing f32 (`NOQUANT`) and 1,093-1,151
  for v1.2.2: about 12 % slower than f32, where #730 measured 7.7x faster on an
  Apple M3 Pro. At 50k, `Q8` gives 107-139 q/s against v1.2.2's 115-116; `BIN`
  is 6.4x faster (732-750 q/s). The int8 distance (`vector::quant::dot_i8`)
  relies on auto-vectorization, and the default x86-64 target has no AVX2;
  the memory gains (3.7x for `Q8`, 16.5x for `BIN`) and recall (equal to
  Redis's) do reproduce. The q/s gate holds today's value.
- `VADD` of `Q8` vectors runs at 52,800-54,900 a second against 62,000-62,600
  for f32 on the same build (and 66,300-78,800 on v1.2.2): quantizing costs
  about 15 % here. The unquantized path is unchanged within noise
  (`f32_768_10k`).
- The costs the PRs accepted show as they said: `XRANGE - + COUNT 10` over
  five-field entries 4-12 % slower than v1.2.2 (82,600-86,400 against
  89,900-96,500 a second; Redis 130,000-131,800); 122.3 B per pending entry
  against 115.9-116.5 (#736: +7 B); 494.1 B per document of a vector-only
  index against 474.5 (#727: +31 B); `HSET` into it within noise. `XADD` of
  five fields is 60 % faster than on v1.2.2 (535,000-579,000 against
  332,000-343,000).
- `redis-cli --cluster reshard` on master is 3.5-4x slower than on Redis
  (25-27 s against 7 s); #724 says the rest is mostly `MIGRATE` itself.

## Not covered

Every row of the gains table in `docs/PERFORMANCE.md` has a suite here (01-13).
From its costs table, these are not re-measured by this suite:

- Instructions per command from callgrind (`GET`/`SET` within 0.1 % for #724,
  the stream commands of #683): that is `scripts/perf/run.sh`, which needs its
  valgrind container. The stream costs are covered here as `redis-benchmark`
  rates (`05` cost rows), which catch a large loss, not a 3-7 % one.
- The load of 3.2M out-of-order stream entries (#683: 0.82 s to 2.0 s): it
  needs the crafted snapshot the PR made, which is not in the repository.
- `EVALSHA` of a script of 20 `string.format` calls (#768: -4.4 %): it needs
  the Lua build. The v1.2.2 `full` build is already fetched by `run-all.sh`;
  a candidate `full` build and an engine pair for it would have to be added.
- The RedisVL checks and the search rows against Redis: they need Redis with
  its Query Engine and JSON modules (`BUILD_WITH_MODULES=yes`, or the
  `redis:8.10` image), which the VM image does not have.
