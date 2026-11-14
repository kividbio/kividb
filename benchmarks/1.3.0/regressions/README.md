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

This is part 1: the runner, the library and suites 01-04. Suites 05-13 are
added the same way (see [Adding a suite](#adding-a-suite)).

| suite | issue / fix | what it measures |
|---|---|---|
| `01-scan-match` | #751 / #769 | full `SCAN` iterations over 2,002,000 keys (`MATCH schedule:*`, no `MATCH`, `*:1*`, `COUNT 10000`), `KEYS schedule:*`; `HSCAN`/`SSCAN`/`ZSCAN` of 200k-element collections as guard rows for #770 |
| `02-growth-ping` | #752 / #780 | worst `PING` while 8M keys are loaded in 2,000-`SET` pipelines; Tokio, and io_uring with `--threads 1`; `FULL=1` adds 17.5M keys with `EX` |
| `03-give-back` | #753 / #763 | RSS 1.5, 5 and 15 s after deleting a 3M-field hash, `HDEL` of 1M fields, and a mixed churn |
| `04-long-commands` | #754 / #757 | `KEYS` over 2M keys, `SCAN COUNT 2000000`, `DEL` of a 3M-field hash, each with 4 `PING` probes; Tokio and io_uring |

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
| `--only 01,04` | only these suites (number or full name) |
| `--engines v1_2_2,candidate` | only these engines; gates that need a missing engine are reported as skipped |
| `--record` | write this run's values into `baseline.json` (see below) |
| `--candidate-commit SHA` | recorded with the values (default: the binary's `--version`) |
| `FULL=1` | also the long configurations (`02-growth-ping/ex17m`: 17.5M keys, about 8 more minutes) |
| `SCALE=0.01` | a smoke run of the code paths with tiny datasets; its numbers mean nothing, never record one |
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
  each binary's path, version and SHA-256, the run time of each suite;
- `results/<label>/raw.tar.gz`: per server run, the metrics with their
  samples, the slowest `PING`s, RSS timelines and the server's log tail.

The exit status is 1 when a gate fails.

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

## Tolerances (gates)

Each metric in `baseline.json` has one gate or a list of them; all must pass.
They are checked against the candidate only.

| kind | rule | used for |
|---|---|---|
| `rel` | within `pct` % of the recorded candidate value, in the worse direction; `abs` adds a slack in the metric's unit (the larger wins); `redis_plus` also allows up to Redis's value this session plus that much | throughput/time: 10 %; memory held: 5 %; RSS after a give-back: 25 % or 5 MiB; worst PING: max(1.5x recorded, Redis + 10 ms); #770 guard rows: 15 % |
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
   stay next to the harness that produced them.
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
- Add its row to the table at the top and its numbers below.

## Recorded run

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
