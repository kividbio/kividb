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
| `14-sinter-big-small` | #759 | `SINTER`/`SINTERCARD`/`SINTERSTORE`/`SDIFF` of a 1M, 5M and (`FULL=1`) 20M-member integer set with a 4,096-member one; `SDIFFSTORE big small` as a cost guard; worst `SADD` to the small set during `SUNION big small` (Tokio, 1M) |
| `15-brpop-many-keys` | #758 | jobs/s delivered to 1,000 and 4,000 workers each in `BRPOP` over 270 queues (and 1,000 over one queue, a guard) while 200 `LPUSH`es go out every 0.1 s; PING p99/max beside it; jobs lost (exactly 0) and clients left blocked after the workers go (0) |
| `16-random-sample` | #839 | `ZRANDMEMBER` / `HRANDFIELD` of a sorted set / hash of 100, 10,000 and 1,000,000 members, with no count, 1, 10, -1, -10, `WITHSCORES` / `WITHVALUES`: microseconds a call (mean of 150-3,000 calls, median of 3 rounds) and the replies checked (length, distinct for a positive count) |
| `17-set-memory` | #297 (step 1), #764 (step 2) | `used_memory` per set member, 1,000,000 members in sets of 10, 100, 1,000 and 100,000, and one set each of 1M, 3M, 3,211,264 and 3,211,265 members (either side of a doubling of the table): 14-byte strings (`membr:<i>`) and integers (`0`..`n-1`); `OBJECT ENCODING` of each size recorded exactly |
| `18-mastodon-feed-fanout` | #849 | Mastodon's home-feed fan-out (FeedManager mix: `ZADD`, `ZREMRANGEBYRANK 0 -801`, `ZREVRANGE 40 40 WITHSCORES`, `ZSCORE`, `EXISTS`, `ZREVRANGEBYSCORE ... LIMIT 0 20`, `ZRANGEBYSCORE`) over 20,000 feeds of 800 members with memtier_benchmark at 200 and 500 connections: ops/s and the timeline read's p50 against Redis and v1.2.2, p99 shown beside them; the same mix paced at 200k requests a second, its read p50 against v1.2.2; `used_memory` and bytes per member; recorded on a `Standard_D8s_v5` (see [the run](#recorded-run-suite-18)) |
| `19-hash-memory` | collection storage step 2, #764 (step 3) | `used_memory` per hash field, 1,000,000 fields in hashes of 10, 100, 1,000 and 100,000, and one hash each of 3M, 3,211,264 and 3,211,265 fields (either side of a doubling of the table): 14-byte fields (`field:<i>`) with 32-byte values, plain and with one field TTL a hash; `OBJECT ENCODING` of each size recorded exactly (`listpack`, `listpackex`, `hashtable`); recorded on a D8s_v5 (`results/s19-branch-d8s_v5`) |
| `20-envoy-ratelimit` | #849 | envoyproxy/ratelimit's pipelined `INCRBY` + `EXPIRE` pair on one key (memtier_benchmark, `--pipeline 2 --transaction`, 100,000 keys) at 200 and 500 connections: ops/s against Redis, p99 shown beside it, and keys left without a TTL (must be 0); on Linux the server and the client are pinned to separate halves of the CPUs; recorded on a `Standard_D8s_v5` (see [the run](#recorded-run-suite-20)) |
| `21-list-memory` | #297 (step 3) | `used_memory` per list element, 1,000,000 32-byte elements in lists of 10, 100, 1,000 and 100,000, packed in chunks; the same made plain (an element over 1,024 bytes pushed and popped first) as guard rows; the same sizes with 6-byte elements (job ids); one list of short elements as a queue leaves it: BullMQ's wait list (ids 1 to 200,000 pushed one `LPUSH` at a time), 200,000 ids after ten full rotations (`LPUSH` one, `RPOP` one), and 1,000,000 drained to 100,000 three times, with `MEMORY USAGE` over the growth of `used_memory` beside each (info); `OBJECT ENCODING` of each size recorded exactly |
| `22-scan-large-count` | #884 | Argo CD's `SCAN <cursor> MATCH revoked-token\|* COUNT 10000` over 40,000 keys of 50 KB that it never matches (`--threads 4`): microseconds a call, client side and the server's own (`INFO commandstats`), calls and keys returned (exactly 0) for a full iteration, and the p99 of four `PING` probes while four other connections loop the same `SCAN` |
| `23-pubsub-pattern-fanout` | #851 | socket.io's Redis adapter: 48 `redis-benchmark` connections `PUBLISH` 200-byte messages to `socket.io#/#room<n>#`, 16 subscribers `PSUBSCRIBE socket.io#/#*` (`pattern`) or `SUBSCRIBE` one channel (`channel`, the plain fan-out guard): messages delivered a second until every subscriber has all of them, messages lost (exactly 0), and the p99 of a probe's own messages from publish to receipt |
| `27-ratelimit-restarted` | #894 | envoyproxy/ratelimit's checks (`INCRBY` + `EXPIRE` on two keys, four commands in one write, 65 connections with 2 ms of the limiter's own time after each) beside 100 connections of pipelined `INCRBY` + `EXPIRE` pairs at 0, 50k and 100k commands a second, on a server restarted from its own snapshot of 100,000 keys (KiviDB keeps the replication backlog then: `repl_backlog_active` is kept in the raw data); of the 4 vCPUs the server on 0 and 2 (`--threads 2`), the checks on 1, the load on 3: checks a second and their p99, every row gated; recorded on a D4s_v5 (`results/pr-894-d4s_v5`). Needs a C compiler (`lib/ratelimit_load.c`). The issue's own shape, with the clients on CPUs of their own up to 300k, needs 8 CPUs: run the suite's load by hand as the PR describes |
| `24-snapshot-size` | #879 | `SAVE` of #879's dataset (`DEBUG POPULATE 6000000 k 150`) and of a collections mix (200,000 hashes of 10 fields, 50,000 each of sets, sorted sets and lists of 20), each twice: `-default` (the candidate as it starts, `rdbcompression no`) and `-yes` (the suite sets `CONFIG SET rdbcompression yes` on the candidate): the snapshot's size, the `SAVE` time (median of 3 after a warm-up) and the time a new server takes to load it (best of 2). Gates: `-yes` keeps 60 % of the size gain over v1.2.2 and #879's dataset within 1.5x of Redis's `dump.rdb`; `-default` within 1 % of v1.2.2's size; the save at most 2.5x v1.2.2's time, the load 1.5x |
| `25-centrifuge-scripts-publish` | #900 | centrifuge v0.40.1's history scripts with the arguments its broker sends (30-day meta TTL), whose 2-3 keys carry no hash tag (each runs alone, on the script-runner thread): `EVALSHA` of `broker_history_list.lua` and `broker_history_add_list.lua` from 16 connections (`scripts`, the `lua,tls` builds); `PUBLISH` to 1,000 channels nobody subscribes to, beside 4 idle subscribers, from 1 connection and from 8, pipeline 16 (`publish`): calls a second counted by the server; a round with any error reply, or a history list of the wrong length, fails the suite. Gates: 10 % and at most 1.05x worse than v1.2.2 in the same session (for now 1.08x for the 8-connection `PUBLISH`, where master is behind v1.2.2 with matching compilers; to go back to 1.05x when that gap is fixed) |
| `26-vector-sets-ann` | #627 | Vector sets on real embeddings (glove-100, and LAION image embeddings at 768 dimensions; 100,000 vectors, and 1,000,000 with `VSET_1M=1`), `Q8` on every server, cosine: `VADD` a second (pipelined, 4 clients), `used_memory` and RSS a vector, then `VSIM ... COUNT 10 EF <ef>` at the first EF whose recall@10 against exact results is at least 0.95 (matched recall, not matched parameters): queries a second and p99 at 1, 16 and 100 connections. Redis 8.10.2 measured the same way, and Garnet v2.2.13 (`$GARNET_SERVER`, `--enable-vector-set-preview`, a preview) in Redis's run, kept in `raw["garnet"]`, not in rows. Gates: `VADD` at least 10x Redis, bytes a vector at most 0.8x, recall at least 0.95, q/s within 25 % of recorded |
| `28-ft-hnsw` | #760 | `FT.CREATE ... VECTOR HNSW` (384-d, COSINE, M 16, EF_CONSTRUCTION 200) and 50,000 random documents written by one client in pipelines of 1,000: documents a second until all are in the graph (FT.INFO `hnsw_backlog` 0); recall@10 against exact and one client's KNN q/s and p99 at `EF_RUNTIME 100`; RSS a document. `pipeline`: the candidate as it starts (`search-index-threads auto`: no helper on the D4s_v5, below 8 CPUs), build within 10 % and at most 1.05x slower than v1.2.2 in the same session. `pipeline-n3`: the candidate with `--search-index-threads 3` (`auto` on 8 CPUs), build within 10 %, 60 % of the gain over v1.2.2 and at most 1.5x Redis's time when `REGRESS_REDISEARCH_SO` points at the query engine module. Both: q/s within 10 %, recall within 2 %, RSS 5 %. `beside` (`--threads 2`, the server on CPUs 0 and 2, so `auto` starts no helper; memtier GET/SET on 1, the loading client and a KNN probe over 8 connections on 3; 20,000 documents, then five rounds of 20 s): GET/SET p99 beside the build and the probe's fastest connection's KNN p50 beside the ingest, each at most 2x v1.2.2's in the same session and within its recorded value (50 % + 0.5 ms, 30 % + 0.2 ms); documents and GET/SET a second shown. Needs memtier_benchmark. Recorded on a D4s_v5 (`results/pr-942-d4s_v5-r3`) |
| `29-unpipelined-commands` | 1.3.0 regression, never released | an API gateway's per-request commands (Tyk's) sent one at a time from 64 connections (`redis-benchmark -c 64`, no `-P`), io_uring and Tokio, against `--threads 1` (`uring1`, `tokio1`) and `--threads 2` (`uring2`, `tokio2`): `SET` of 385 bytes, `GET`, `INCR`, `PTTL`, `PUBLISH` of 177 bytes to no subscriber, a second each from the server's own `total_commands_processed`; the server pinned to its first CPUs, the client to the next (at most 6). Gate: each rate at most 1.05x worse than v1.2.2 in the same session; a row where the client used 95 % of its CPUs is shown, not gated (on a `Standard_D4s_v5`, the two-worker rows); `min_cpus=4`: not run on a smaller machine |
| `30-script-call-cost` | #901 | server CPU per call and calls a second of short scripts by `EVALSHA` (`redis-benchmark`, 32 connections, one KiviDB worker): Tyk's sliding-log limiter over 5,000 keys of 144 members, the compare-and-delete lock release of #880 on keys another owner holds, and `return 1`, each also with its keys under a hash tag. Needs a `lua,tls` candidate (`--candidate-full`, #921). Gates: CPU per call within 10 % of the recorded value and at most 0.93x v1.2.2's for Tyk, 1.03x for the others; calls a second 10 % |
| `31-zset-memory` | #825, #297 (step 4) | `used_memory` per sorted-set member, 1,000,000 members in sets of 10, 100, 128, 129 and 1,000: 14-byte members (`membr:<i>`), scores whole (`int`) or with a fraction (`float`); `OBJECT ENCODING` of each size recorded exactly (`listpack` up to 128, `skiplist` past it); the large sets (129 and 1,000) as guard rows |
| `32-default-threads` | #903 | the default worker count itself: unpipelined `GET` (50 connections), `SET` pipeline 16, suite 05's `XADD` and suite 23's pattern fan-out, each with no `--threads` and with every explicit count from 1 to the CPUs, server and clients sharing the machine; `<row>_default_vs_best` (default / best) recorded but informational (the default is half the CPUs, rounded up, since the change that re-recorded it; it may become a gate at 0.90); `pubsub_lost` exact |
| `33-ft-aggregate-groupby` | #761 | #761's 2,000,000 hashes (`name TEXT email TAG communityNumber NUMERIC age NUMERIC`, pipelines of 2,000): `FT.AGGREGATE userIdx * GROUPBY 1 @communityNumber REDUCE COUNT 0 AS n LIMIT 0 10`, the same with `REDUCE AVG 1 @age` and `SORTBY 2 @n DESC`, and the first over `@age:[20 40]`: median ms of 5 after a warm-up, and the reply's group total (exactly 1000); `used_memory` and RSS once loaded, the peak RSS (`VmHWM`) after the queries, and FT.INFO's memory parts as info rows. v1.2.2 and the candidate only: the VM image's Redis has no Query Engine |
| `34-kong-ratelimit` | #849 | Kong's rate-limiting plugin with `policy: redis`: its per-request script (`incr`, and `expire` when the count is 1; Kong 3.9.4's `usage()`, verbatim) sent as Kong sends it (`EVAL` with the script text) and by `EVALSHA`, on one key, `ratelimit:<route>:<service>:<identifier>:<period_date>:minute`, from memtier_benchmark at 200 and 500 connections over 100,000 identifiers; with the key as Kong builds it (`plain`) and with the identifier under a hash tag (`tag`), in two configurations: `default` (`lua-parallel-scripts yes`) and `serial` (`lua-parallel-scripts no` on KiviDB). ops/s against Redis, p99 shown, keys left without a TTL (must be 0); every call's key checked under MONITOR first, for each way of calling. Gates: at least 1.2x Redis at 200 connections and 1.3x at 500 in every configuration and key form. Needs 8 vCPUs and the `lua,tls` builds (`--candidate-full`) |
| `35-tokio-pipelined-reads` | #432 / #891 | the Tokio loop (`--io-model tokio --threads 1`, the server on the first CPU, `redis-benchmark --threads 3` on the next three, 50 connections): `SET` of 1,024-byte values at pipeline 16 and of 4,096-byte values at pipeline 32, a second from the server's own `total_commands_processed` (median of 3); and one connection with a 64 KiB receive buffer that sends 400,000 `SET`s of 20-byte values (19 MB) in one write before reading a reply: 1 if it is written within 20 s and every reply arrives. Gates: the rates 10 % and 60 % of the gain over v1.2.2 in the same session; the pipeline `>= 1` (v1.2.2 stalls). Recorded on a D4s_v5 (`results/pr-891-d4s_v5`) |
| `36-litellm-ratelimit` | #871 | LiteLLM's proxy rate limiter: its five Lua scripts byte for byte (`litellm/rust/crates/redis-store/src/rate_limits/lua.rs`, main @ 4b12a405; SHA-1s checked against `SCRIPT LOAD`), called as one request calls them, by `EVALSHA`, from memtier_benchmark (`--transaction`: one key and its team a call) at 200 and 500 connections over 50,000 keys: `BATCH_RATE_LIMITER` for one descriptor (one hash tag) and for key + team (two), `CHECK_AND_INCREMENT_BY_N` and `PARALLEL_ACQUIRE` for key + team, and the 13-command per-request mix. A 22-call transcript checked exactly first (window reset, all-or-nothing over the limit, the reconcile, acquire and release, TTLs), and every script call's keys under MONITOR; the candidate must run at least 90 % of its scripts beside others (`INFO stats` `scripts_run_beside_others`), or the suite fails. ops/s against Redis (a floor per workload, see the docstring), p99 shown, keys left without a TTL (must be 0). Needs 8 vCPUs and the `lua,tls` builds (`--candidate-full`) |
| `37-zrandmember-by-count` | #862 | `ZRANDMEMBER` of a large sorted set (a table and a tree) of 1,000, 100,000 and 1,000,000 members: no count, 1, 10, 1000, -10, -1000, each count with and without `WITHSCORES`; the server's own microseconds a call (`INFO commandstats`, reset before each case) and the client's wall time, median of 3 rounds; replies checked (length, distinct for a positive count). Gates: server time 100 % or 3 us (the same build measured 66 and 110 us on two D4s_v5s), and at 100,000 and 1,000,000 at most 3x Redis for counts of ±10 and ±1000; wall time 50 % or 20 us, and at 100,000 and 1,000,000 60 % of the gain over v1.2.2. See [the run](#recorded-run-suite-37) |

## Running it

Linux only (it reads `/proc/<pid>/status`, and io_uring needs a kernel and a
sandbox that allow it: not Docker's default seccomp profile). Run it on a VM,
not on a laptop, and nothing else on the machine.

```sh
rustup update stable    # the Rust release v1.2.2's binaries were built by, or later: see below
cargo build --release
cargo build --release --features lua,tls --target-dir target-full
benchmarks/1.3.0/regressions/run-all.sh --candidate target/release/kividb \
    --candidate-full target-full/release/kividb \
    --candidate-commit "$(git rev-parse --short HEAD)" --label master-$(git rev-parse --short HEAD)-d4s_v5
```

**Build the candidate with the Rust release the reference was built by.**
The release workflow builds with the stable Rust of the day (v1.2.2: rustc
1.100.0, on Amazon Linux 2023). A VM image's older toolchain is not that: on
2026-11-18 the same master tree built by rustc 1.99.1 ran centrifuge's script
rows 10-13 % and its one-connection `PUBLISH` row 4-7 % slower than built by
1.100.0, and v1.2.2 built by 1.99.1 was as slow; an earlier comparison of the
two read that as a regression of master (#900). `env.json` records the rustc
of every KiviDB binary (from its `.comment` section), `baseline.json` records
the candidate's with each value (`recorded.rustc`, `rustc_full`), and
`compare.py` judges it against v1.2.2's (and the `full` pair's):

- **older** than v1.2.2's, or **unknown** (no `rustc version` in the binary):
  the run is `INVALID`, because its rows against v1.2.2 compare compilers;
- **newer**: a `WARN` line only (`rust-toolchain.toml` says `stable`, so a
  later release is normally built by a newer rustc);
- `REGRESS_ALLOW_RUSTC=1` turns the `INVALID` cases into warnings, for a
  deliberate check of the compiler's effect. Run that way in #921, the master
  `lua,tls` build by rustc 1.99.1 was `REGRESSED` against v1.2.2's on suite 25's
  script rows by 8-18 % (not kept as a result: it measures the compiler).

| option | |
|---|---|
| `--candidate <path\|tag>` | a kividb binary, or a release tag such as `v1.3.0` (its default-build tarball, SHA-256 checked) |
| `--candidate-full <path\|tag>` | the candidate built with `--features lua,tls` (or a tag: its `full` tarball), for the configurations that need scripts (`Config(lua=True)`: `25-centrifuge-scripts-publish/scripts`); they run v1.2.2's `full` build against it. Without it they are skipped, listed in `summary.md`, and not expected |
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

Suite 26 also uses h5py (pinned in `requirements.txt`), downloads its two
datasets once into `$VSET_DATASETS` (default `$REGRESS_CACHE/datasets`:
glove-100-angular from ann-benchmarks.com, 485 MB, and
laion-img-emb-768-1M-cosine from redis/vector-db-benchmark's list, 3.1 GB,
both over https and SHA-256 checked), and needs `GARNET_SERVER`, the
GarnetServer of Garnet v2.2.13 (`linux-x64-based.tar.xz`, SHA-256
`23a4f19d5ffd1c533f1db7cc9ef4c00a5f811aa2914544cc819fd23215fa557c`, on .NET
10), or `GARNET_SERVER=none` to leave Garnet out. Its 1,000,000-vector
configurations run only with `VSET_1M=1`, not with `FULL=1`: Redis loads
them at under 1,000 `VADD`/s, so they need `SUITE_TIMEOUT=7200`.

Suites 05-13 also use `redis-benchmark` and `redis-cli` from the directory of
the Redis binary, numpy (pinned in `requirements.txt`, for
`../vector-sets/bench.py`), a second venv for RedisVL (`10-redisvl`,
`$REGRESS_CACHE/venv-redisvl`, from `../../redisvl-compat/requirements.txt`)
and a `redis-cli` 7.0.x for `13-cluster-reshard` (`$REDIS_CLI_7`, else
`/usr/bin/redis-cli` if it is 7.0.x, as Ubuntu 24.04's `redis-tools` is, else
built from Redis's 7.0.15 tag into `$REGRESS_CACHE`, which needs git, make
and a C compiler). Suites 18, 20 and 34 use `memtier_benchmark` 2.6.1 or newer
(`--transaction` and its error-reply lines; `$MEMTIER`, else on `PATH`).

## Method

- One server at a time, started fresh for each configuration, in an empty
  directory, with no persistence (`--save ''`/`--appendonly no` for Redis;
  KiviDB saves nothing by default). KiviDB runs with `--protected-mode no
  --bind 127.0.0.1`, `--io-model` as the configuration says (`auto`
  otherwise), `--threads` as it says (every configuration states its
  thread count: see [Thread counts](#thread-counts)) and
  `enable-debug-command yes` in a `--configfile` (v1.2.2 has no flag for
  it). Redis runs with its defaults (`io-threads 1`, jemalloc) and
  `--enable-debug-command yes`.
- Client and server share the machine. Absolute numbers depend on it; the
  gates that compare with v1.2.2 or Redis measure both in the same session,
  and the gates against recorded values are only meaningful on the VM size
  they were recorded on (`recorded.vm`). A different VM size is `INVALID`.
  The same size can still land on another CPU generation (two
  `Standard_D4s_v5` differed by about 40 %: f32 `VSIM` 639-679 q/s against
  1,104-1,124), so speed gates are judged against v1.2.2 of the same session,
  see "Host speed" below. A different CPU model is not `INVALID`:
  `env.json` has `cpu`, `nproc` and `calib_ms` (best of 3 of 200k chained
  sha256 on one core; indicative only, not used by any gate) and `summary.md`
  shows the recorded and the current CPU model.
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

### Thread counts

The default worker count is a policy, not code under test: #903 changed it
from the CPUs minus two (all of them below four) to one per available CPU,
which on the `Standard_D4s_v5` is 4 workers instead of 2; a later change made it half the
available CPUs, rounded up (2 on a `Standard_D4s_v5`, 4 on a `Standard_D8s_v5`; never fewer than 2 where there are 2 or more CPUs). With the clients
on the same 4 vCPUs that moved rows that `--threads` did not pin: `05`
`shapes/xadd5_rps` 594k/s with 2 workers, 426k with 4; `08`
`ftinfo/hset_vec_doc_rps` 98.6k against 89.1k. So every configuration now
states its count (`Config(..., threads=N)`; `lib/suite.py` refuses one that
does not), and it is the count its values were recorded with, so no value
was re-recorded for it:

| suites / configurations | `--threads` | why |
|---|---:|---|
| `01`-`06`, `08`-`12`, `14`-`17`, `19` (every configuration that had none) | 2 | recorded before #903 (commits `2468f226`, `d66f0f93`, `9501caa6`, `219b9656`, `f767a149`, `9c466db3`, `8160700c`, `5b4a04a4`, `c7e0b47e`, `36771af1`: none contains it), where the default on 4 vCPUs was 2 (the servers' logs in `raw.tar.gz` say `Worker threads : 2`) |
| `24-snapshot-size`, `31-zset-memory` | 4 | recorded on branches that had merged #903: the candidate's logs say `Worker threads : 4 (auto: one per available CPU)`. v1.2.2 ran with its default of 2 there and runs with 4 now: `24`'s v1.2.2 values are only used by its same-session `vs_v1_2_2` gates on the `SAVE`/load time, and `31` gates memory |
| `02 uring1`, `07`, `13` | 1 | as before |
| `22` | 4 | as before |
| `23`, `25`, `27` | 2 | as before |
| `18-mastodon-feed-fanout` | 8 | recorded on a `Standard_D8s_v5` (`min_cpus=8`) with the default of the day: the candidate's log says `Worker threads : 8 (auto: one per available CPU)` |
| `20-envoy-ratelimit` | the server's pinned CPUs (4) | as before: it pins the server to half the CPUs and passes `--threads` to match |
| `30-script-call-cost` | 1 | as before |
| `21-list-memory` (all four configurations) | 4 | re-recorded in #940 with the candidate at its default of the day on 4 vCPUs (4 workers, after #903) |
| `33-ft-aggregate-groupby` | 4 | as recorded (#944) |
| `34-kong-ratelimit` | the server's pinned CPUs (4) | as suite `20`: the server is pinned to half the CPUs of a `Standard_D8s_v5` and gets `--threads` to match |
| `35-tokio-pipelined-reads` | 1 | as recorded: one worker on its own CPU, the client on the other three |
| `36-litellm-ratelimit` | the server's pinned CPUs (4) | as suites `20` and `34` |
| `37-zrandmember-by-count` | 2 | as suite `16`, whose sizes it extends |
| `32-default-threads` | 0 (none) | measures the default on purpose |

Redis ignores the count. The default itself is watched by
`32-default-threads` alone: a change of the default policy shows there, and
nowhere else.

## Tolerances (gates)

Each metric in `baseline.json` has one gate or a list of them; all must pass.
They are checked against the candidate only.

| kind | rule | used for |
|---|---|---|
| `rel` | within `pct` % of the recorded candidate value, in the worse direction; `abs` adds a slack in the metric's unit (the larger wins); `redis_plus` also allows up to Redis's value this session plus that much | throughput/time: 10 %; memory held: 5 %; RSS after a give-back: 25 % or 5 MiB; worst PING: max(1.5x recorded, Redis + 10 ms); #770 guard rows: 35 % (measured spread up to 28 %) |
| `ratio` | the gain over v1.2.2 measured this session (v1.2.2/candidate for lower-is-better) is at least `keep` x the recorded gain | every claimed improvement: keep 60 % |
| `vs_redis` | candidate at most `max_x` times worse than Redis this session | SCAN iterations: 1.3x |
| `vs_v1_2_2` | candidate at most `max_x` times v1.2.2's value this session (below 1: that much better) | `KEYS user:99*` (04): 0.85x |
| `bound` | absolute: `eq` (`"recorded"`: equal to the recorded value), `max`, `min` | counts (exact); worst PING on Tokio during a long command: 20 ms |
| `info` | shown, never gated | call counts, PINGs over 10 ms, probes stalled |

### Host speed

v1.2.2 is re-measured in every session, so it tells how fast this host is
compared with the one the baseline was recorded on. A **speed-dependent** metric
is one whose unit is a time or a rate: `ms`, `s`, `us`, `ns` or anything ending
in `/s`. A gate can say `"speed": false` (or `true`) to override the unit rule.
Bytes, MiB, counts, connections, recall and ratios are never scaled.

For a `rel` gate on a speed-dependent metric, `compare.py` scales the recorded
candidate value by an effective factor `f` before applying `pct`/`abs`: rates
are multiplied by `f`, times divided by it (1.00 = same host, 0.6 = this host is
40 % slower). `abs` is not scaled. `own` is the metric's own v1.2.2
now/recorded (rates) or recorded/now (times), from that entry's own recorded
v1.2.2 value; `grp` is the median of `own` over the speed metrics of the same
suite/configuration (at least 3 of them, else the median over all suites).

| case | f | rule in `summary.json` |
|---|---|---|
| v1.2.2 has no usable value for the metric (missing, 0, NaN) | `grp` | `group` / `global` |
| `own` < `grp` / 1.3: this metric slowed more than its group | `own` | `own-slower` |
| `own` > `grp` x 1.3: it slowed less than its group | `min(own, max(grp, 1))` | `own-capped` |
| otherwise | `grp` | `group` / `global` |
| no v1.2.2 value in the whole run | 1 (compared absolutely; `summary.md` and the console say so) | `none` |

A gate can force the rule: `"anchor": true` always uses `own`, `"anchor": false`
never does (`set_rps_beside_ft_info` has it: v1.2.2's rate there varies 3x). The
summary lists every metric whose `own` and `grp` differ by more than 1.3x.

Two real cases decided these rules:

- `09-vector-sets` f32 on a `Standard_D4s_v5` where v1.2.2 `VSIM` ran 639-679
  q/s against 1,124 recorded, while its `VADD` was unchanged (65.4k against
  66.4k/s). The config's `grp` is 0.57 (VSIM-bound), so `VSIM` is judged at 0.57
  and passes. A `VADD` that had collapsed to 27k/s is judged at its own 0.98
  (`own-capped`: it did not slow, so it gets no excuse) and fails.
- `13-cluster-reshard` `reshard_s`: v1.2.2 does it by another, 10x slower
  method, and its v1.2.2 time was 1.43x faster than recorded on the same host as
  everything else. `min(own, max(grp, 1))` keeps it at 1.0: one noisy v1.2.2
  number never makes a gate stricter than the recorded value.

Sanity: if an `own` or a `grp` that is used for scaling is outside 0.4-2.5 (v1.2.2
was starved, or the host is of another class), the run is `INVALID` and names the
metric: rerun. A higher-is-better `rel` gate whose `abs` is not below the
(scaled) recorded value is a configuration error and also `INVALID`.

Scheduling jitter is not CPU speed. The PING latency rows (`02-growth-ping`
`ping_p99_ms`/`ping_max_ms`, `04-long-commands` `*_worst_ping_*`/`*_least_worst_ping_ms`,
`07-save-ping` `ping_during_save_*`) are marked `"speed": false` in the suites
(`jitter()` in `lib/suite.py`) and in `baseline.json`: they are neither scaled
nor counted toward a factor. `04` and `07` are `bound` gates anyway.

Still absolute: `bound` gates (a worst PING of at most 20 ms is a promise to
users, not a ratio), `eq` counts, all memory gates, and `info`. `ratio` and
`vs_redis` already compare within the session. If the global factor is under
0.7 or over 1.4, the summary adds a `WARN` line (not a failure): the bounds were
recorded on a faster or slower host. `--record` stores the CPU model in
`recorded.cpu` (and the candidate's rustc in `recorded.rustc`). Re-run the
self-tests with `python lib/test_compare.py` and `python lib/test_suite25.py`
(suite 25's error check) after changing this logic.

Why a few rows differ from the plan's defaults:

- `15-brpop-many-keys` c1000 `ping_max_ms`: the worst PING of 1,000 blocked
  workers beside a producer and a probe on 4 vCPUs is scheduling jitter. Five
  runs of the master build on a `Standard_D4s_v5` gave 31.9-43.6 ms
  (median 37.7) for the candidate, 39.6-48.8 ms (median 42.0) for Redis and
  1,125-1,486 ms for v1.2.2 (`results/flaky15-r1` to `r5`). The first
  recording, 32.2 ms with the usual 1.5x, failed healthy runs; the row now
  records the median of the five and is judged at 2x it or Redis + 10 ms
  (75 ms), still 15x under v1.2.2's. The other two configurations keep 1.5x.
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
- `04-long-commands` `keys_cmd_ms`: its recorded value (161 ms, master
  `2468f226`) was 15 % slower than v1.2.2; #789 made it about 0.6x v1.2.2's
  time. Rather than re-record, the gate was tightened by hand: besides 10 %
  over the recorded value, it must stay at most 0.85x v1.2.2's time measured
  in the same session (0.61x in #789's runs, so the noise of a few % is far
  from it; losing about half the gain fails).
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
- `09-vector-sets` `VSIM` q/s: rel 25 % (#730 saw 15 % between repeats). Q8 at
  10K also keeps 60 % of its gain over v1.2.2's f32 search, measured within the
  run: since #793 (run-time AVX-512BW / AVX2 int8 kernel) it is 3.7x on this VM
  size; before it, Q8 was 0.9x and had no gain to keep.

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
   `run-all.sh --candidate v1.3.0-rcN --candidate-full v1.3.0-rcN --label v1.3.0-rcN-d4s_v5`
   (`--candidate-full` takes the RC's `full` tarball, or a `lua,tls` build of
   the same commit; without it suite 25's `scripts` is skipped and the release
   gate refuses the run). The Actions workflow (below) builds both. Suites
   that declare `min_cpus` above the machine's 4 CPUs are not run there
   (`needs_more_cpus` in `env.json`, their metrics "not run").
2. For those suites, the same candidate with `FULL=1` on a machine with at
   least that many CPUs (`Standard_D8s_v5` for `min_cpus=8`):
   `run-all.sh --candidate v1.3.0-rcN --candidate-full v1.3.0-rcN --min-cpus-only --label v1.3.0-rcN-d8s_v5`.
   Their baseline values were recorded on that machine (`recorded.vm`), and
   each metric is judged on the machine size it was recorded on. Host-speed
   scaling (below) works per run: each run's factors come from the v1.2.2
   values of the suites it ran, against their own recorded ones, so the two
   machine sizes never mix. With no suite declaring `min_cpus`, this step
   runs nothing and is not needed.
3. Every gate must pass in both. A failure is either a regression to fix
   before the release, or an intended change that is explained in the
   release PR, after which the baseline is re-recorded.
4. Commit both `results/<label>/` with the release PR, so the release's
   numbers stay next to the harness that produced them. The release workflow
   refuses a stable tag unless the committed runs together cover every suite
   (`scripts/release/check-regression-results.sh vX.Y.Z --ref HEAD`, or with
   `--runs v1.3.0-rcN-d4s_v5,v1.3.0-rcN-d8s_v5` to judge just those: runs
   with verdict `ok` whose candidate commit is an ancestor of the tag, with
   `src/`, `Cargo.toml`, `Cargo.lock` and `rust-toolchain.toml` unchanged
   since, and every `min_cpus` suite run on a machine that big; it names
   every suite no run covers). The Actions workflow "Regression suite"
   (`.github/workflows/regression-suite.yml`) makes both machines (the
   second with `big`, its default), builds the candidate on each, runs the
   suite and uploads each `results/<label>/` as an artifact;
   docs/RELEASING.md has the steps.
5. For a new improvement in the release, its suite and its first recorded
   values land with the fix's PR (or right after it), not at release time.

## Adding a suite

Checklist (the release gate refuses a tree that misses any of these):

- [ ] The suite's number is free (directory and open PRs).
- [ ] If its load generator is the limit on the `Standard_D4s_v5`, it declares
      `S = Suite("NN-name", [...], min_cpus=8)`; its README text runs it with
      `run-all.sh --min-cpus-only`, not `--only NN`.
- [ ] Its baseline is recorded (`--record`) **on the machine it declares**: a
      `Standard_D4s_v5` without `min_cpus`, a `Standard_D8s_v5` with
      `min_cpus=8` (`--min-cpus-only --record`). A suite with no rows in
      `baseline.json` fails the gate ("no recorded values"), and so does a
      run whose rows say `new`.
- [ ] Recorded with a candidate built by the release's rustc (or newer), and
      without `ALLOW_VM_CHANGE`/`REGRESS_ALLOW_RUSTC`: the gate refuses runs
      that used either.
- [ ] Every configuration states `threads=N` (`lib/suite.py` refuses one
      that does not, and the run is then `INVALID`): the count you record
      it with, so a later change of the default worker count does not move
      its rows. Only `32-default-threads` uses `threads=0` (the default).
- [ ] Its row in the table above, its gates explained in its docstring.

A suite is `suites/NN-name.py`. `run-all.sh` picks up every file named
`[0-9][0-9]-*.py`, in order.

```python
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib"))
from suite import Suite, Config, THROUGHPUT, MEMORY, WORST_PING, KEEP_IMPROVEMENT, EXACT, rel, bound, info, median, scaled
from probe import Probe   # PING probes
from rss import vmrss_mib, used_memory_mib, sample_at

S = Suite("05-example", [
    Config("tokio", io="tokio", threads=2),                       # all three engines
    Config("uring1", io="uring", threads=1, engines=("v1_2_2", "candidate")),
    Config("big", threads=2, full_only=True),                    # FULL=1 only
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
- A suite whose load generator, not the server, is the limit on the
  `Standard_D4s_v5` declares the CPUs it needs, in one line:
  `S = Suite("18-example", [...], min_cpus=8)`. `run-all.sh` then does not
  run it on a smaller machine (`needs_more_cpus` in `env.json`: not run, not
  `INVALID`); `--only 18` runs it anyway and makes the run `INVALID` ("not
  valid for the gate"); `--min-cpus-only` on a big enough machine runs only
  such suites. Record its baseline on that machine
  (`run-all.sh --min-cpus-only --record` on a `Standard_D8s_v5`): its
  metrics then carry that `recorded.vm`, and the release gate needs its run
  from such a machine (see "How a release uses it").
- A configuration that needs Lua says `Config(..., lua=True)`: `run-all.sh`
  then starts v1.2.2's `full` build (fetched to
  `$REGRESS_CACHE/releases/v1.2.2/full/kividb`, SHA-256 checked) and the
  `--candidate-full` binary under the usual engine names `v1_2_2` and
  `candidate`, and skips the configuration when `--candidate-full` is not
  given. `env.json` lists both binaries as `v1_2_2_full` and `candidate_full`.
- Smoke it (`SCALE=0.01 run-all.sh --only 05 ...`), then run it for real with
  `--only 05 --record`, and twice more without `--record` to see the noise;
  widen a tolerance only for noise you measured.
- A suite that needs several servers declares its configuration
  `Config(..., self_hosted=True)` and starts them with `lib/nodes.py`
  (`with Nodes(ctx) as ns: nodes = ns.cluster(3)`); `ctx.bin` is the engine's
  binary and `ctx.port`/`ctx.pid` are None. A suite that needs one more
  server beside run-all.sh's uses `ns.start()` the same way.
- Record it (`--record`) unless every gate it emits is session-relative
  (`vs_v1_2_2`, `vs_redis`, `info`): the release gate refuses a suite with no
  rows in `baseline.json` (`scripts/release/regression_gate.py`, `unrecorded()`)
  except such a session-only one (`session_only()`), which needs nothing
  recorded. One row with a recorded gate (`rel`, `ratio`, `bound`) and the
  suite must be recorded like any other. Recording freezes each gate as the
  suite declared it then, so a suite that picks a row's gate at run time
  (`29-unpipelined-commands`: client-limited rows are `info`) must stay
  session-only. A run covers a session-only suite only if at least one of
  its `vs_v1_2_2`/`vs_redis` gates passed and none was skipped (no v1.2.2 or
  Redis value): rows that are all `info` judge nothing, and a session-only
  suite no run judged is refused like an unrecorded one (`session_exempt()`).
- State `threads=` on every `Config`.
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
| 04-long-commands / tokio | keys_cmd_ms (ms) | 141.1 | 161.3 | 216.5 | rel 10%; <= 0.85x v1.2.2 |
| 04-long-commands / tokio | keys_worst_ping_max_ms (ms) | 132.1 | 8.09 | 205.1 | max 20 |
| 04-long-commands / tokio | keys_worst_ping_ms (ms) | 129.3 | 5.94 | 203.6 | max 20 |
| 04-long-commands / tokio | scan_cmd_ms (ms) | 254.3 | 112.5 | 471.7 | rel 10% |
| 04-long-commands / tokio | scan_worst_ping_max_ms (ms) | 243.4 | 5.19 | 467.2 | max 20 |
| 04-long-commands / tokio | scan_worst_ping_ms (ms) | 241.0 | 0.45 | 461.2 | max 20 |
| 04-long-commands / uring | del_cmd_ms (ms) | 136.4 | 134.3 | - | rel 10% |
| 04-long-commands / uring | del_least_worst_ping_ms (ms) | 0.21 | 0.41 | - | rel 50% or +10 |
| 04-long-commands / uring | del_worst_ping_ms (ms) | 135.8 | 132.9 | - | info |
| 04-long-commands / uring | keys_cmd_ms (ms) | 145.6 | 153.6 | - | rel 10%; <= 0.85x v1.2.2 |
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
  as fast. Fixed in #789 (below).
- The incremental table growth (#780) raised the growth load's PING p99 on
  Tokio from 2.0 to 3.0 ms (and 0.7 to 1.5 ms on io_uring with one thread),
  while its worst PING fell from 112 to 9.7 ms.
- `SCAN 0 MATCH user:99* COUNT 2000000` takes 112 ms instead of 254-265 ms.

### #789: `KEYS` with a selective pattern

`KEYS` matched every key in place through the general glob matcher
(`glob_redis::match_from`): 136 instructions a key for `user:99*`, 76 % of
the command. v1.2.2 copied every key and checked each one's time to live as
well, twice the instructions, and was still faster: matching interleaved
with the walk over 2M entries costs more time than its instruction count
says. Since #789 `KEYS` makes the pattern ready once (`glob_redis::Glob`,
as `SCAN` does), tells most keys by their first or last byte, and writes
matches straight into the reply. Not related to #757: the loss was the same
on io_uring, where `long_work` does nothing.

Instructions of one `KEYS` over `DEBUG POPULATE 2000000 user` (callgrind,
whole server, Tokio):

| pattern | v1.2.2 | master `5809ac61` | #789 |
|---|---:|---:|---:|
| `user:99*` (11,111 keys) | 697.9 M | 358.5 M | 184.9 M |
| `schedule:*` (none) | 491.3 M | 150.1 M | 110.0 M |

Wall time on a D4s_v5, median of 7 (9 for suite 01's keyspace), one engine
at a time:

| | v1.2.2 | master `5809ac61` | #789 |
|---|---:|---:|---:|
| `KEYS user:99*`, Tokio, no other client | 137-139 ms | 143-145 ms | 83-84 ms |
| same, with suite 04's four PING probes | 137-138 ms | 153-155 ms | 87 ms |
| same, io_uring, no other client | 136-138 ms | 142-146 ms | 83-84 ms |
| suite 01's keyspace: `KEYS schedule:*` | 113 ms | 57 ms | 54 ms |
| suite 01's keyspace: `KEYS user:99*` | 135 ms | 144 ms | 82 ms |
| suite 01's keyspace: `KEYS *:1999` | 203 ms | 309 ms | 59 ms |

Suites 01 and 04 with the fix (`results/pr789-d4s_v5`, compared with the
baseline recorded above, the new gate included): every gate passed;
`keys_cmd_ms` 90.2 ms against v1.2.2's 136.9 on Tokio (0.66x) and 87.0
against 141.8 on io_uring (0.61x), `keys_schedule_ms` 53.7 ms (recorded
60.4), and the `SCAN` rows within their recorded values. Two more runs of
the same build before the gate was added gave 90.0/87.9 ms (0.61x/0.61x)
and 97.5/94.3 ms (0.72x/0.67x, before the comparison was inlined). The probes' extra 10 ms on master is the PINGs being served
meanwhile, which is #757's point: v1.2.2 served none.

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
- `simd-9501caa6-d4s_v5-09` and `simd-9501caa6-d4s_v5-09-rerun`: suite 09 on
  #793's branch, on a D4s_v5 about 40 % slower at f32 `VSIM` than the one
  above (v1.2.2: 639-679 q/s against 1,104-1,124), so the unchanged f32 row
  failed its absolute q/s gate in both while the f32 code did not change.
  Only `q8_768_10k` `vsim_qps` was re-recorded from the first, with the new
  ratio gate; the rerun is its noise check.

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
| the same after #793 (another D4s_v5, slower: v1.2.2 f32 639-679) | #793 | 686 / 2,621 / 6,960 (HNSW) | 679 / 2,493 / 6,732 (HNSW) |
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
| 09-vector-sets / q8_768_10k | vsim_qps (q/s) | 678.6 | 2,493 | 6,732 | rel 25%; keep 60% of gain |
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

## Recorded run, suite 14

Recorded 2026-11-14 on an Azure Standard_D4s_v5 (Xeon Platinum 8370C, a
fresh VM of the same image), `FULL=1 --only 14 --record` with the harness of
master `6297d818` (host-speed rules); the candidate is commit `219b9656` of
#798, `cargo build --release`. Runs: `pr759-d4s_v5` (recorded) and
`pr759-d4s_v5-rerun` (the noise check: every gate passed, host factor
1.01x). About 6.5 minutes with `FULL=1`, most of it loading 20M members into
each engine and the `SDIFFSTORE` rows.

Medians of 5 timed runs (ms; `SUNION` 3), recorded run:

| N | command | v1.2.2 | candidate | Redis 8.10.2 | gate |
|---|---|---:|---:|---:|---|
| 1M | `SINTER big small` | 95.75 | 2.33 | 2.56 | rel 50% or 2 ms; keep 60% of gain; <= 2x Redis |
| 1M | `SINTER small big` | 69.5 | 2.09 | 2.30 | same |
| 1M | `SINTERCARD 2 big small` | 92.7 | 0.18 | 0.46 | rel 50% or 2 ms |
| 1M | `... LIMIT 100` | 93.2 | 0.055 | 0.063 | rel 50% or 2 ms |
| 1M | `SINTERSTORE dst big small` | 93.7 | 0.30 | 0.85 | rel 50% or 2 ms |
| 1M | `SDIFF small big` | 69.9 | 1.95 | 2.57 | as `SINTER` |
| 1M | `SDIFFSTORE dst big small` | 288.1 | 253.6 | 314.2 | rel 25% (cost guard) |
| 5M | `SINTER big small` | 535.3 | 2.22 | 3.87 | as above |
| 5M | `SINTER small big` | 446.3 | 2.06 | 3.52 |  |
| 5M | `SINTERCARD 2 big small` | 533.3 | 0.22 | 1.56 |  |
| 5M | `... LIMIT 100` | 533.4 | 0.054 | 0.12 |  |
| 5M | `SINTERSTORE dst big small` | 533.3 | 0.48 | 2.52 |  |
| 5M | `SDIFF small big` | 447.2 | 2.03 | 3.94 |  |
| 5M | `SDIFFSTORE dst big small` | 1,645 | 1,424 | 1,825 |  |
| 20M | `SINTER big small` | 2,573 | 2.29 | 4.76 |  |
| 20M | `SINTER small big` | 2,431 | 2.10 | 3.82 |  |
| 20M | `SINTERCARD 2 big small` | 2,568 | 0.30 | 1.89 |  |
| 20M | `... LIMIT 100` | 2,571 | 0.057 | 0.13 |  |
| 20M | `SINTERSTORE dst big small` | 2,573 | 0.38 | 2.84 |  |
| 20M | `SDIFF small big` | 2,457 | 2.01 | 4.70 |  |
| 20M | `SDIFFSTORE dst big small` | 7,993 | 6,899 | 8,067 |  |
| 1M, Tokio | `SUNION big small` | 1,330 | 1,331 | 1,305 | rel 25% |
| 1M, Tokio | worst `SADD small` during it | 362.7 | 0.48 | 321.9 | max 20 ms (jitter: not scaled) |

The replies (2,097 members at 1M and 20M, 2,098 at 5M; 1,998-1,999 for
`SDIFF`; the `SDIFFSTORE` and `SUNION` sizes) are the same on all three
engines and gated exactly. The candidate's spread between the two runs:
-8 to +4 % on `SINTER`/`SDIFF`, 0 to +3 % on `SDIFFSTORE`, -1 % on
`SUNION`, and -22 to +4 % on the count-only rows (0.05-0.48 ms: round-trip
noise, hence no ratio or Redis gate on them); the worst `SADD` was 0.48 and
0.43 ms. The `SINTER`/`SDIFF` times no longer depend on N: what is left is
the round trip and redis-py building a Python set of ~2,000 members.
`SDIFFSTORE big small` and `SUNION` walk all of `big` on every engine; they
are there so that cost does not grow, and so a writer of the small set is
not held up by the walk (v1.2.2 and Redis hold it for the whole command).

## Recorded run, suite 15

Recorded 2026-11-15 on an Azure Standard_D4s_v5 (Xeon Platinum 8370C),
`--only 15 --record`; the candidate is commit `f767a149` of #822 (#758),
`cargo build --release`. Runs: `pr822-d4s_v5` (recorded) and
`pr822-d4s_v5-rerun` (the noise check: every gate passed). About 5.5 minutes:
3 configurations x 3 engines, each 8 s of warm-up, 20 s measured and the
drain.

Workers are raw sockets in two processes, each sending `BRPOP <270 queues in
an order of its own> 2` again as soon as it is answered; the producer sends
200 `LPUSH queue:<random> job` in one pipeline every 0.1 s (at most about
2,000 jobs/s offered, as in the issue). Recorded run (rerun in brackets):

| workers x queues | metric | v1.2.2 | candidate | Redis 8.10.2 | gate |
|---|---|---:|---:|---:|---|
| 1,000 x 270 | jobs/s | 380 (375) | 1,934 (1,931) | 1,567 (1,569) | rel 10 %; keep 60 % of the gain; <= 1.5x Redis |
| 1,000 x 270 | worst PING | 973 (1,015) ms | 32 (23) | 38 (36) | max(1.5x recorded, Redis + 10 ms) |
| 4,000 x 270 | jobs/s | 8 (39) | 1,924 (1,919) | 950 (975) | rel 10 %; <= 1.5x Redis |
| 4,000 x 270 | worst PING | 20,224 (5,919) ms | 72 (57) | 216 (174) | max(1.5x recorded, Redis + 10 ms) |
| 1,000 x 1 | jobs/s | 1,920 (1,935) | 1,950 (1,950) | 1,838 (1,838) | rel 10 % (guard) |
| 1,000 x 1 | worst PING | 3.0 (2.8) ms | 1.9 (2.4) | 12.2 (11.5) | max(1.5x recorded, Redis + 10 ms) |

The candidate delivers what is offered at every size: the rate is the
producer's, which is why no gate is scaled by the host's speed (`jitter()`),
and the worst `PING` is scheduling, not CPU speed, so it is not scaled
either. v1.2.2's 4,000 x 270 rate is 8-39 jobs/s from run to run, so that
row has no ratio gate (one failed a rerun on v1.2.2's noise alone, the
candidate unchanged). The p99 `PING` is shown, not gated: candidate 4-7 ms,
Redis 27-101, v1.2.2 0.4-9.3 s. Every engine delivered every job exactly
once (`lost` 0) and left no client blocked once the workers had gone
(`blocked_after` 0; the issue's 469 left on 1.2.2 were not reproduced here,
with workers that close cleanly).

## Recorded run, suite 16

Recorded 2026-11-16 on an Azure Standard_D4s_v5 (Xeon Platinum 8370C),
`--only 16 --record`; the candidate is commit `8160700c` (#839),
`cargo build --release --features lua,tls`. Runs: `pr-839-d4s_v5` (recorded)
and `pr-839-d4s_v5-rerun` (the noise check: every gate passed, host speed
1.03x). A run takes 75 s.

One connection, `redis-py`, one reply at a time: each figure is a mean over
150 (1,000,000 members), 1,000 (10,000) or 3,000 (100) calls, the median of 3
rounds, and includes the round trip, about 50 us here. `bad_replies` (a reply
of the wrong length, or with a member twice for a positive count) is 0 on
every engine. Recorded run (rerun in brackets):

1,000,000 members / fields (microseconds a call, rerun in brackets):

| command | v1.2.2 | candidate | Redis 8.10.2 |
|---|---:|---:|---:|
| `ZRANDMEMBER z` | 5,608 (5,537) | 1,121 (1,065) | 46 (47) |
| `ZRANDMEMBER z 1` | 6,560 (6,210) | 995 (1,056) | 49 (50) |
| `ZRANDMEMBER z 10` | 6,443 (6,375) | 1,852 (1,871) | 84 (64) |
| `ZRANDMEMBER z -10` | 5,470 (5,426) | 1,803 (1,877) | 79 (79) |
| `ZRANDMEMBER z 10 WITHSCORES` | 6,544 (6,272) | 1,820 (1,860) | 90 (72) |
| `HRANDFIELD h` | 66 (50) | 49 (67) | 50 (48) |
| `HRANDFIELD h 1` | 1,096 (1,055) | 51 (71) | 66 (50) |
| `HRANDFIELD h 10` | 1,049 (1,135) | 63 (60) | 80 (63) |
| `HRANDFIELD h -10` | 700 (695) | 60 (59) | 79 (80) |
| `HRANDFIELD h 10 WITHVALUES` | 1,111 (1,211) | 70 (69) | 88 (72) |

10,000 members / fields (microseconds a call, rerun in brackets):

| command | v1.2.2 | candidate | Redis 8.10.2 |
|---|---:|---:|---:|
| `ZRANDMEMBER z` | 95 (84) | 60 (68) | 50 (47) |
| `ZRANDMEMBER z 1` | 78 (98) | 75 (59) | 58 (66) |
| `ZRANDMEMBER z 10` | 113 (107) | 72 (69) | 78 (79) |
| `ZRANDMEMBER z -10` | 108 (99) | 74 (69) | 59 (59) |
| `ZRANDMEMBER z 10 WITHSCORES` | 110 (98) | 79 (99) | 85 (69) |
| `HRANDFIELD h` | 66 (48) | 48 (66) | 58 (47) |
| `HRANDFIELD h 1` | 83 (57) | 70 (71) | 65 (50) |
| `HRANDFIELD h 10` | 94 (90) | 63 (80) | 60 (77) |
| `HRANDFIELD h -10` | 63 (87) | 79 (79) | 75 (75) |
| `HRANDFIELD h 10 WITHVALUES` | 75 (75) | 76 (89) | 77 (84) |

100 members / fields (microseconds a call, rerun in brackets):

| command | v1.2.2 | candidate | Redis 8.10.2 |
|---|---:|---:|---:|
| `ZRANDMEMBER z` | 50 (65) | 48 (50) | 47 (53) |
| `ZRANDMEMBER z 1` | 51 (54) | 51 (52) | 51 (56) |
| `ZRANDMEMBER z 10` | 59 (71) | 59 (59) | 59 (60) |
| `ZRANDMEMBER z -10` | 65 (60) | 59 (60) | 59 (75) |
| `ZRANDMEMBER z 10 WITHSCORES` | 87 (68) | 68 (68) | 68 (71) |
| `HRANDFIELD h` | 48 (54) | 49 (48) | 47 (48) |
| `HRANDFIELD h 1` | 61 (51) | 69 (51) | 50 (56) |
| `HRANDFIELD h 10` | 60 (64) | 77 (59) | 59 (67) |
| `HRANDFIELD h -10` | 65 (59) | 59 (80) | 59 (75) |
| `HRANDFIELD h 10 WITHVALUES` | 78 (74) | 68 (67) | 71 (71) |

What it shows:

- **The hash is O(count)**, as in Redis: at 1,000,000 fields `HRANDFIELD h 10`
  takes 63 us against 1,049 us in v1.2.2 (Redis 80), whatever the size. At 100 and
  10,000 there was nothing to gain and nothing is lost (the figures are the
  round trip).
- **The sorted set is not.** It has no index by position (a hash map of
  members beside an ordered tree), so a sample is found by walking the member
  table up to the last position picked, without copying it: about half of it
  for one member (about a millisecond at 1,000,000, v1.2.2: 5.6 ms), nearly
  all of it for ten (1.8 ms, v1.2.2: 6.4 ms). Redis, which indexes by rank,
  takes 50-90 us. At 10,000 members the walk is within the round trip. A
  member table that can be asked for its Nth member, as the sets' is
  (`member_set.rs`) or an order-statistic tree, would make it O(count), and is
  the storage change this needs.

Gates: `rel` 50 % or 20 us (whichever is larger) on every row; at 1,000,000
also `ratio` (keep 60 % of the gain over v1.2.2, every row but `HRANDFIELD h`,
which never copied) and, for the hash rows, at most 2x Redis. The sorted set's
rows are not compared with Redis (see above). The figures are of the client's
round trip as much as of the server: compare within a session, as the gates do.

## Recorded run, suite 17

Recorded 2026-11-17 on an Azure Standard_D4s_v5 (Xeon Platinum 8370C),
`--only 17 --record`; the candidate is commit `5b4a04a4` (#297 step 1, built
on #309), `cargo build --release`. Runs: `pr-packed-sets-5b4a04a4-d4s_v5`
(recorded) and `pr-packed-sets-5b4a04a4-d4s_v5-rerun` (the noise check:
every gate passed; the figures agree to 0.001 B). A run takes 71 s.

Bytes of `used_memory` a member, 1,000,000 members in sets of each size
(`OBJECT ENCODING` of the candidate's sets in brackets; Redis's are the same):

| members a set | v1.2.2 strings | candidate strings | Redis 8.10.2 strings | v1.2.2 integers | candidate integers | Redis 8.10.2 integers |
|---:|---:|---:|---:|---:|---:|---:|
| 10 | 75.6 | 34.2 (listpack) | 24.8 | 67.6 | 17.2 (intset) | 8.8 |
| 100 | 55.4 | 20.3 (listpack) | 18.5 | 47.4 | 3.7 (intset) | 2.8 |
| 1,000 | 67.0 | 67.0 (hashtable) | 30.2 | 59.0 | 59.0 (hashtable) | 22.0 |
| 100,000 | 54.3 | 54.3 (hashtable) | 36.8 | 46.3 | 46.3 (hashtable) | 28.8 |

The 1,000 and 100,000 rows are guards: sets past the block's and the array's
limits are tables, as in v1.2.2, and must not grow. Instructions per command
are not measured here (the suite has no callgrind harness); #297's PR has
them from `scripts/perf`-style runs.

### Re-recorded for #764 step 2 (large sets on `CollectionTable`)

Recorded 2026-11-20 on an Azure Standard_D4s_v5 (Xeon Platinum 8370C),
rustc 1.100.0, `cargo build --release`: the candidate is commit `99467f18`
(run `s17-branch-99467f18-d4s_v5`), against master `0caf6055` measured the
same way in the same session (`s17-master-0caf6055-d4s_v5`, not recorded).
Only the changed and new rows were taken into `baseline.json`: every
`bytes_per_member_*` row, and every row of the new sizes; the `encoding_*`
and `load_*_s` rows of 10 to 100,000 members are as recorded before. The
suite now also has one set each of 1,000,000, 3,000,000, 3,211,264 and
3,211,265 members (the last two either side of the table's doubling from
2^19 to 2^20 groups), and reads `used_memory` once the server has stopped
giving memory back (a growth finished in the background frees its old
array at its end).

| members a set | v1.2.2 strings | master strings | candidate strings | Redis 8.10.2 strings | v1.2.2 integers | master integers | candidate integers | Redis 8.10.2 integers |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 10 | 75.6 | 34.2 | 31.8 | 24.8 | 67.6 | 17.2 | 14.8 | 8.8 |
| 100 | 55.4 | 20.3 | 20.1 | 18.5 | 47.4 | 3.75 | 3.51 | 2.8 |
| 1,000 | 67.0 | 67.0 | 34.0 | 30.0 | 59.0 | 59.0 | 18.0 | 21.9 |
| 100,000 | 54.3 | 54.3 | 29.8 | 36.8 | 46.3 | 46.3 | 13.8 | 28.8 |
| 1,000,000 | 67.7 | 67.7 | 33.5 | 30.1 | 59.7 | 59.7 | 17.5 | 22.1 |
| 3,000,000 | 55.8 | 55.8 | 29.7 | 37.9 | 47.8 | 47.8 | 13.7 | 35.2 |
| 3,211,264 (fullest) | 54.2 | 54.2 | 29.4 | 36.8 | 46.2 | 46.2 | 13.4 | 34.3 |
| 3,211,265 (just grown) | 54.2 | 54.2 | 37.2 | 36.8 | 46.2 | 46.2 | 21.2 | 34.3 |

The block and the array (10 and 100 members) are the same structures as
before; each set key is 24 bytes smaller because a set's header (`MemberSet`)
went from 64 to 40 bytes with the table boxed (2.4 B a member at 10, 0.24 at
100). `OBJECT ENCODING` is unchanged at every size.

## Recorded run, suite 18

Recorded 2026-11-18 on an Azure Standard_D8s_v5 (Xeon Platinum 8370C),
the values of `pr870-d1a6712c-d8s_v5-rerun2` (`compare.py --record`, with
`--allow-vm-change`); the candidate is master `d1a6712c` (this PR adds only
the suite), `cargo build --release` with rustc 1.100.0 (the compiler of the
v1.2.2 release binary: a build by 1.99.1 would look 5-13 % slower);
memtier_benchmark 2.6.1; Redis 8.10.2 from the VM image. Four runs, all with
verdict `ok`: `pr870-d1a6712c-d8s_v5` (the first: its 500-connection master
rate, 231,000, is the low outlier below, so the baseline takes the second),
`-rerun2` (recorded), `-rerun3`, and `-verify` (a fresh run against the
recorded values and the final gates). A run takes about 10 minutes. Nothing is
pinned: the server (8 threads) and memtier share the 8 vCPUs.

Three runs (the range over them):

| | v1.2.2 | master | Redis 8.10.2 | master / Redis | master / v1.2.2 |
|---|---:|---:|---:|---:|---:|
| ops/s, 200 connections | 253,000-261,000 | 254,000-261,000 | 126,000-131,000 | 1.98-2.02x | 1.00-1.03x |
| ops/s, 500 connections | 245,000-264,000 | 231,000-261,000 | 124,000 | 1.86-2.11x | 0.945-0.988x |
| timeline read p50, 200, ms | 0.54-0.57 | 0.70-0.71 | 1.46-1.48 | 0.48x | 1.25-1.30x |
| timeline read p50, 500, ms | 1.36-1.48 | 1.79-2.00 | 3.85-3.94 | 0.46-0.51x | 1.31-1.35x |
| mix p99, 200 / 500, ms | 3.9-4.0 / 5.7-6.1 | 1.4-1.6 / 3.4-4.0 | 2.9-3.0 / 7.5-7.7 | | |
| `used_memory`, MiB (bytes per member) | 1,727 (113.2) | 1,727 (113.2) | 1,214 (79.5) | 1.42x | 1.00x |

What is true and not flattering: master is not ahead of v1.2.2 in the mix's
rate (the same within noise, 500 connections up to 5.5 % below it in the low
run), its timeline read p50 is 25-35 % worse than v1.2.2's (still half of
Redis's), and a member costs 42 % more memory than in Redis, as in v1.2.2.
What it does better is p99: a third to two thirds of v1.2.2's, and below
Redis's. So the gates are "at least 1.75x Redis" (6 % under the lowest
ratio measured), "at most 10 % under v1.2.2" (`vs_v1_2_2` 1.1: the
runs went up to 5.5 % under it), the read p50 at most 0.55x Redis's and 15 %
over the recorded one (master's read p50 against v1.2.2 was recorded, not
gated, at first: see "The read p50 against v1.2.2" below), 10 % against the recorded rate and 5 % on the memory. This suite is
noisier than 20: a 10 % loss in the rate is caught by the `v1.2.2` gate only
at its edge.

Which side is the limit. Master used 346 % of CPU on average (v1.2.2 378 %)
and memtier's four threads were at 100 % of a CPU each (400 %) in 29 of 87
samples, the rest of the time Redis's runs, warm-ups and the load: server and
client together fill the 8 vCPUs, so the mix's rate is bounded by the client
and by the machine, not by the server alone. That the two builds are equal
here says the client is the limit; the p50 and p99 are the rows that see the
server.

Verified 2026-11-19 on a fresh Standard_D8s_v5 (the same CPU) against the master
`7fd67dbb` (#921 merged), `rustc 1.100.0`, `run-all.sh --min-cpus-only`
(`results/pr870-7fd67dbb-d8s_v5-verify`): verdict `ok`, host speed 0.97x; 200 / 500
connections 248,500 / 246,800 ops/s against Redis's 115,100 / 96,700. The
recorded rows carry `recorded.rustc` 1.100.0, the compiler of the recorded run's
`env.json`.

### The read p50 against v1.2.2

Bisected 2026-11-19 on a D8s_v5 (fat LTO, rustc 1.100.0, each first-parent
merge built in its own target dir, five alternating runs of this mix at 200
connections, the md5 of every binary checked). The v1.2.2 tag built on the
VM and the release binary measure the same (p50 0.48-0.50 ms, p99 3.84-3.87).

| build | read p50, ms | mix p99, ms | ops/s | mean, ms | `ZREVRANGEBYSCORE`, us a call |
|---|---:|---:|---:|---:|---:|
| v1.2.2 | 0.50 [0.49-0.51] | 3.86 | 280,000 | 0.71 | 19.4 |
| `59583540` (before #712) | 0.50 [0.48-0.50] | 3.86 | 280,000 | 0.71 | 19.0 |
| `e585c821` (#712) | 0.62 [0.61-0.63] | 1.18 | 296,000 | 0.67 | 6.0 |
| `e3a8d2c5` (before #903) | 0.63 [0.61-0.64] | 1.22 | 294,000 | 0.68 | |
| `e8a0fdda` (#903) | 0.65 [0.64-0.66] | 1.43 | 282,000 | 0.71 | |
| master `05b3ffec` | 0.65 [0.64-0.66] | 1.43-1.46 | 282,000 | 0.70 | 4.9 |
| master, `--threads 6` | 0.62 [0.61-0.62] | 1.17 | 292,000 | 0.68 | |
| master + 14 us of busy work in each by-score read | 0.50 [0.46-0.53] | 3.82 | 275,000 | 0.72 | 22.8 |
| master without #712's integer scores | 0.44 [0.41-0.53] | 3.97 | 286,000 | 0.70 | 23.1 |

Flat out, memtier is the limit and the mean latency is fixed by connections
/ ops/s, so a lower p99 at the same rate means a higher median. #712 replies
a whole-number score as an integer (Redis's format, #472; a feed's scores
are status ids, whole numbers of 15 digits) instead of through `{:e}` and a
string rebuilt from it: the 20-member `WITHSCORES` page became 3x cheaper,
the workers sleep between requests (voluntary context switches 123k to 257k
a run) instead of running behind, and the 4 ms waits of connections queued
behind a busy worker are gone. Making the read as dear again on master, by
busy work or by taking the integer path out, gives back v1.2.2's p50, and
its p99 with it. #903 (one worker per CPU: 8 here, where v1.2.2 started 6)
adds 0.02-0.03 ms with memtier on the same 8 vCPUs; `--threads 6` gives it
back. Neither is reverted: one is Redis's reply format and a 3x cheaper
read, the other #903's choice for a server with CPUs of its own. #877 and
#938 do not move this row.

With the server not saturated the two are equal: at 200k requests a second
(`--rate-limiting=1000` per connection) the read p50 is 0.46-0.53 ms on
master against 0.48-0.51 on v1.2.2 (p99 1.12 / 1.11 ms), and 0.29 ms both
at 100k. Pinned (server on CPUs 0-3 with `--threads 4`, memtier on 4-7) it
is 0.663 against 0.655; with the server held to 2 workers (the server the
limit) master is ahead: 0.83 ms against 0.98, at 230k ops/s against 192k.

So the suite gates the flat-out read p50 at most 1.5x v1.2.2's in the same
session (it is 1.28-1.35x), and adds the paced run, whose read p50 is gated
at most 1.15x v1.2.2's: that row sees a read that got dearer or replies held
longer, which the flat-out one cannot tell from a fairer schedule.

The paced rows (`ops_paced`, `read_p50_ms_paced`, `read_p99_ms_paced`) are
recorded from `results/p50-c153b8d0-d8s_v5` (master `c153b8d0`, rustc 1.100.0,
a D8s_v5 with a Xeon Platinum 8573C; `run-all.sh --only 18`, verdict `ok`, its
summary judged with these gates): read p50 0.527 ms against v1.2.2's 0.495
(1.06x), p99 1.10 against 1.13; flat out 0.647 / 0.511 ms (1.27x) and 1.66 /
1.29 ms (1.29x) at 200 / 500 connections. Redis serves 128k a second flat out,
so its paced row is a flat-out one. The other rows keep their recorded values.

This suite declares `min_cpus=8`: the normal run on the D4s_v5 of the other
suites does not run it (`needs_more_cpus` in `env.json`), and `--only 18` on it
makes the run `INVALID`. Run it on a D8s_v5 with `run-all.sh --candidate <bin>
--candidate-full <bin> --min-cpus-only --label <label>-d8s_v5` (see "How a
release uses it").

## Recorded run, suite 19

Recorded 2026-11-17 on an Azure Standard_D4s_v5 (Xeon Platinum 8370C),
`--only 19 --record`; the candidate is commit `c7e0b47e` (small hashes in one
block), `cargo build --release`. Runs: `pr-packed-hashes-d4s_v5` (recorded)
and `pr-packed-hashes-d4s_v5-rerun` (the noise check: every gate passed; the
figures agree to 0.2 B). A run takes 84 s.

Bytes of `used_memory` a field, 1,000,000 fields of 14 bytes with 32-byte
values in hashes of each size (`OBJECT ENCODING` of the candidate's hashes
in brackets; Redis's are the same):

| fields a hash | v1.2.2 plain | candidate plain | Redis 8.10.2 plain | v1.2.2 one TTL | candidate one TTL | Redis 8.10.2 one TTL |
|---:|---:|---:|---:|---:|---:|---:|
| 10 | 162.4 | 71.4 (listpack) | 56.8 | 192.2 | 101.2 (listpackex) | 72.1 |
| 100 | 129.2 | 55.8 (listpack) | 51.8 | 131.5 | 58.1 (listpackex) | 62.3 |
| 1,000 | 166.6 | 166.6 (hashtable) | 70.4 | 166.9 | 166.9 (hashtable) | 70.4 |
| 100,000 | 128.8 | 128.8 (hashtable) | 76.8 | 128.9 | 128.9 (hashtable) | 76.8 |

The 1,000 and 100,000 rows are guards: hashes past the block's limits are
tables, as in v1.2.2, and must not grow. Instructions per command are not
measured here; the PR has them from callgrind runs of the same builds.

## Recorded run, suite 20

Recorded 2026-11-18 on an Azure Standard_D8s_v5 (Xeon Platinum 8370C),
`--only 20 --record` with `ALLOW_VM_CHANGE=1`; the candidate is master
`d1a6712c` (this PR adds only the suite), `cargo build --release` with rustc
1.100.0 (the compiler of the v1.2.2 release binary: a build by 1.99.1 would
look 5-13 % slower); memtier_benchmark 2.6.1; Redis 8.10.2 from the VM image.
Runs: `pr887-d1a6712c-d8s_v5` (recorded) and `-rerun2`, `-rerun3` (the noise
check: every gate passed). A run takes 447 s. The server has CPUs 0-3 and memtier 4-7 (the
suite's own split).

Three runs (the range over them; the recorded run is inside it):

| connections | v1.2.2 ops/s | master ops/s | Redis 8.10.2 ops/s | master / Redis | master / v1.2.2 |
|---:|---:|---:|---:|---:|---:|
| 200 | 568,000-571,000 | 565,000-574,000 | 289,000-291,000 | 1.94-1.99x | 0.989-1.008x |
| 500 | 546,000-550,000 | 544,000-549,000 | 283,000-286,000 | 1.90-1.92x | 0.989-1.005x |

p99 (shown, not gated): 1.03-1.05 ms against Redis's 2.7 ms at 200
connections, 2.7-2.9 ms against 6.8 ms at 500. `keys_without_ttl` was 0 on all
three engines.

Master is not ahead of v1.2.2 here: the two run within 2 % of each other, and
both are 1.9-2.0x Redis. So the gates are "at least 1.8x Redis" (5 % under
the lowest ratio measured, so a loss of 10 % fails), "at most 8 % under v1.2.2"
(`vs_v1_2_2` 1.08, the within-session gate that would catch a loss), and 10 %
against the recorded rate. The 1.5x and 1.7x placeholders came from an M3
laptop.

Which side is the limit. On a `Standard_D4s_v5` (server on 2 CPUs, memtier
on 2) master, v1.2.2 and Redis are all near 280-310k ops/s: master 0.86-0.94x
Redis, v1.2.2 the same. There memtier used about 83 % of its two CPUs and
KiviDB 87-90 % of its two, so neither side had room and the ratio said
nothing about the server; Redis, one thread at 95-100 % of a CPU, was the only
one limited by its server. That is why the suite is recorded on 8 vCPUs.
On the D8s_v5 (server on 4 CPUs) KiviDB uses 337-344 of 400 % and memtier's four
threads are at 100 % of a CPU each in most samples while they drive it (400 %
in 40 of 59), so
KiviDB's rate here is a floor under what it can do, and the ratio to Redis
a lower bound: the gates are sound against losses, but a server that gets
10 % faster does not show. A larger client (more memtier threads, more CPUs)
would measure the server's own limit. KiviDB is 2x Redis here because it
uses four threads to Redis's one: per CPU it handles about 140,000 ops/s, Redis
290,000.

KiviDB's `MONITOR` feed is not lossless under load (the suite's check: 3,040 of
56,867 pairs looked as if on two keys with four connections, 0 of 20,000 with
one): the check runs on one connection and tolerates 5 %.

Verified 2026-11-19 on a fresh Standard_D8s_v5 (the same CPU) against the master
`7fd67dbb` (#921 merged), `rustc 1.100.0`, `run-all.sh --min-cpus-only`
(`results/pr887-7fd67dbb-d8s_v5-verify`): verdict `ok`, host speed 0.94x; 200 / 500
connections 544,000 / 509,000 ops/s against Redis's 268,000 / 234,000 (2.03x,
2.17x) and v1.2.2's 549,000 / 499,000. The recorded rows carry `recorded.rustc`
1.100.0, the compiler of the recorded run's `env.json`.

This suite declares `min_cpus=8`: the normal run on the D4s_v5 of the other
suites does not run it (`needs_more_cpus` in `env.json`; the Redis gate would
fail by design there), and `--only 20` on it makes the run `INVALID`. Run it on a
D8s_v5 with `run-all.sh --candidate <bin> --candidate-full <bin> --min-cpus-only
--label <label>-d8s_v5` (see "How a release uses it").

## Recorded run, suite 23

Recorded 2026-11-18 on an Azure Standard_D4s_v5 (Xeon Platinum 8370C),
`--only 23 --record`; the candidate is commit `37c98d80` (#851, merged with
master `aee34f90`), `cargo build --release`. Runs: `pr851-37c98d80-d4s_v5`
(recorded) and `pr851-37c98d80-d4s_v5-rerun` (the noise check: every gate
passed). A run takes 140 s.

| config | metric | v1.2.2 | candidate | Redis 8.10.2 |
|---|---|---:|---:|---:|
| pattern | delivered a second | 1,306,995 | 1,467,643 | 1,366,055 |
| pattern | p99 publish to receipt, ms | 0.75 | 0.77 | 0.92 |
| channel | delivered a second | 1,508,943 | 1,468,950 | 1,429,524 |
| channel | p99 publish to receipt, ms | 0.86 | 0.68 | 0.86 |

KiviDB runs with `--threads 2` here (see the suite's docstring): with the
default of one worker a CPU, its 4 workers share the 4 vCPUs with the
clients, and master delivered 1.15M a second with a 4.3 ms p99 in the
pattern row against Redis's 1.35M and 0.9 ms. The channel row is a guard:
the 4 Python readers are near their limit there, so it catches a loss, not
a gain. #851's PR has the pinned measurements (server on 2 vCPUs, clients
on the other 2, a Rust client), where the difference is larger.

## Recorded run, suite 26

Recorded 2026-11-19 on an Azure Standard_D4s_v5 (Xeon Platinum 8370C),
`--only 26 --record` (`GARNET_SERVER` set, `SUITE_TIMEOUT=3600`); the
candidate is commit `6065c780` (#922 merged with master `5bd40b4b`),
`cargo build --release` by rustc 1.100.0, KiviDB on `--threads 4`. Runs:
`pr922-6065c780-d4s_v5` (recorded; the 100,000-vector configurations only,
`VSET_1M` unset) and `pr922-6065c780-d4s_v5-rerun` (the noise check: every gate
passed, host speed 1.04x). A run takes 759 s. The rerun's candidate `VSIM` was
1.5x faster than the recorded run's (193 / 344 / 369 a second on glove100, 56 /
142 / 143 on laion768), Redis's 1.0-1.25x: this suite's `VSIM` rates move more
between runs than the 25 % gate, and the recorded values are the lower run's.

| config | metric | v1.2.2 | candidate | Redis 8.10.2 |
|---|---|---:|---:|---:|
| glove100_100k | `VADD` a second | 179,699 | 177,837 | 1,935 |
| glove100_100k | `used_memory` a vector, bytes | 496 | 196 | 869 |
| glove100_100k | EF, recall@10 | 10, 1.000 | 10, 0.985 | 640, 0.971 |
| glove100_100k | `VSIM` a second at 1 / 16 / 100 connections | 81 / 196 / 187 | 125 / 279 / 277 | 463 / 1,182 / 1,166 |
| glove100_100k | `VSIM` p99 ms at 1 / 16 / 100 | 14.1 / 106 / 1,059 | 9.7 / 78 / 873 | 3.8 / 26 / 174 |
| laion768_100k | `VADD` a second | 102,935 | 89,907 | 1,600 |
| laion768_100k | `used_memory` a vector, bytes | 3,168 | 864 | 1,526 |
| laion768_100k | EF, recall@10 | 10, 0.998 | 10, 0.975 | 80, 0.966 |
| laion768_100k | `VSIM` a second at 1 / 16 / 100 connections | 25 / 63 / 64 | 54 / 132 / 133 | 1,631 / 4,284 / 4,036 |
| laion768_100k | `VSIM` p99 ms at 1 / 16 / 100 | 42.8 / 297 / 3,002 | 19.7 / 162 / 1,793 | 0.94 / 7.3 / 42.3 |

KiviDB's `VSIM` is an exact scan (#710), so it needs no EF to reach the target
and Redis's HNSW search is 3.7-30x faster on it at 100,000 vectors: the rows
record that, they do not gate it against Redis. What is gated against Redis
is where KiviDB is ahead: `VADD` (0.01x and 0.02x of Redis's time per vector,
limit 0.1x) and memory (0.23x and 0.57x, limit 0.8x); recall is at least
0.95 everywhere. The `VSIM` q/s rows hold within 25 % of the recorded value.
v1.2.2's own `VSIM` is the same scan, 0.5-0.7x as fast, so the speed gates of
this first recording were judged against the recorded values only.

Garnet v2.2.13 (`raw["garnet"]` in `raw.tar.gz`, not a row), measured the same
way: `VADD` 3,833 and 3,087 a second; EF 1,280 (recall 0.960) on glove100 and 80
(0.971) on laion768; `VSIM` 187 / 567 / 568 a second on glove100 and
1,244 / 4,070 / 4,213 on laion768 at 1 / 16 / 100 connections; RSS a vector
10,517 and 14,005 bytes (it has no `used_memory`). It needs the .NET 10 runtime
(`DOTNET_ROOT`; the tarball is framework-dependent).

## Recorded run, suite 29

Run 2026-11-19 on an Azure Standard_D8s_v5 (Xeon Platinum 8370C) held to
4 vCPUs (`taskset -c 0-3 run-all.sh --only 29`, so the suite sees what a
`Standard_D4s_v5` gives it), default builds, rustc 1.100.0. Not recorded
into `baseline.json`: every gate is against v1.2.2 in the same session, and
the rows that are gated depend on the machine (see "client-limited" below).
Runs: `s29-master-1-4cpu` (master `16da886f`, before the fix: `REGRESSED`,
the five `uring1` rows), `s29-fin2-2-4cpu` and `s29-fin2-3-4cpu` (the fix:
`ok` twice). A run takes about 17 minutes.

| config | metric | v1.2.2 | master | fix (2 runs) |
|---|---|---:|---:|---:|
| uring1 | `GET` a second | 140,575 | 123,222 (1.14x worse) | 141,460 / 142,411 |
| uring1 | `SET` a second | 138,398 | 125,494 (1.10x) | 140,410 / 140,455 |
| uring1 | `INCR` a second | 142,126 | 129,305 (1.10x) | 142,186 / 141,634 |
| uring1 | `PTTL` a second | 146,795 | 128,952 (1.14x) | 144,641 / 140,803 |
| uring1 | `PUBLISH` a second | 146,000 | 131,320 (1.11x) | 142,371 / 145,229 |
| tokio1 | `GET` a second | 109,109 | 107,966 | 107,964 / 107,981 |
| uring2 | `GET` a second, client-limited | 164,597 | 165,644 | 164,035 / 164,161 |

On 4 vCPUs the two-worker rows (`uring2`, `tokio2`) are client-limited:
redis-benchmark's two CPUs run at 2.0 and the server's two at about 1.93,
and master measured the same as v1.2.2 there, so they are shown, not
gated. On 8 vCPUs (six client CPUs) they are gated, and there master ran
9-10 % behind v1.2.2 as well (the PR's Go client: `GET` 216k against 238k).
v1.2.2's own samples of a row on one worker spread by up to 7 % (the client
shares the server's core), so a median 1.05x worse is the smallest loss the
gate catches reliably; master's was 1.10-1.14x.

## Recorded run, suite 30

Recorded 2026-11-19 on an Azure Standard_D4s_v5 (4 vCPU, Xeon Platinum **8573C**:
this VM came with a newer CPU than the 8370C of most other baselines, and
`recorded.cpu` says so), `--only 30 --record`, the candidate master `7fd67dbb`
(#921 merged; it holds #928), the default `lua,tls` build by rustc 1.100.0 (the
compiler of the v1.2.2 release binaries), Redis 8.10.2 from the VM image.
One KiviDB worker. Runs: `pr934-7fd67dbb-d4s_v5` (recorded), `-rerun2`, `-rerun3`
(the noise check; every gate passed; a run takes 549 s).

CPU per call, microseconds (the three runs; the recorded value is the first):

| workload | v1.2.2 | master | Redis 8.10.2 | master / v1.2.2 | gate |
|---|---:|---:|---:|---:|---:|
| Tyk's sliding log | 14.9-15.1 | 12.5-12.6 | 12.0-12.5 | 0.82-0.84x | 0.93x |
| compare-and-delete lock | 9.3-9.7 | 8.8-9.1 | 8.0-8.2 | 0.93-0.95x | 1.03x |
| `return 1` | 8.2-8.4 | 8.0 | 6.7-7.2 | 0.96-0.98x | 1.03x |
| Tyk, keys under a hash tag | 14.9-15.4 | 12.8-13.5 | 12.1-12.5 | 0.86-0.87x | 0.93x |
| lock, hash tag | 9.6-9.7 | 9.0-9.4 | 7.7-8.2 | 0.94-0.97x | 1.03x |
| `return 1`, hash tag | 8.5-8.6 | 8.3-8.7 | 7.0-7.3 | 0.97-1.01x | 1.03x |

The calls a second move by 0.7-5.4 % between the runs (10 % against the recorded
value is the gate). The gates against v1.2.2 hold in all three runs; the
nearest is the `return 1` row under a hash tag: 1.012x in the first run, 0.980x
and 0.966x in the others, so a 1.03x gate is 1.8 % above the worst of three
while the three differ by 4.6 %.

## Recorded run, suite 33

Recorded 2026-11-19 on an Azure Standard_D4s_v5 (Xeon Platinum 8370C),
`--only 33 --record`; the candidate is commit `6f4dc275` (#761: `GROUPBY` as
the documents are read, and index terms, tags and keys held in place),
`cargo build --release` by rustc 1.100.0, both engines on `--threads 4`. Runs:
`pr-761-6f4dc275-d4s_v5` (recorded) and `pr-761-6f4dc275-d4s_v5-rerun` (the
noise check: every gate passed, host speed 1.00x). A run takes 146 s.

| metric | v1.2.2 | candidate (recorded / rerun) | Redis 8.10.3 | gate |
|---|---:|---:|---:|---|
| `count_ms` | 3,179 | 913 / 913 | 1,907 | rel 10%; keep 60% of gain |
| `avg_ms` | 3,581 | 1,033 / 1,027 | 2,428 | rel 10%; keep 60% of gain |
| `filtered_ms` | 1,185 | 358 / 355 | 759 | rel 10%; keep 60% of gain |
| `used_memory_mib` | 2,607 | 1,224 / 1,224 | 1,454 | rel 5%; keep 60% of gain |
| `rss_loaded_mib` | 3,238 | 1,488 / 1,478 | 1,509 | rel 5%; keep 60% of gain |
| `rss_peak_mib` | 4,785 | 1,543 / 1,531 | 2,158 | rel 10%; keep 60% of gain |
| `*_groups` | 10 | 1,000 | 1,000 | exact |

The Redis column is not part of the suite: it is the median of 3 runs of the
same workload on the same VM against the `redis:8` image (8.10.3, `--save ""
--appendonly no`, in Docker on CPUs 0-2, the client on CPU 3), and master
`f8cc49ee` before this change took 3,453 / 3,885 / 1,308 ms, 1,755 MiB of
`used_memory`, 2,171 MiB of RSS once loaded and a peak of 3,722 MiB in the same
runs. v1.2.2's group total is the page's 10 rows (#795 fixed it). The peak is
the server's whole RSS high-water mark, load included: v1.2.2 and master made
a row (a map of every field, as strings) of each of the 2,000,000 documents
before grouping them, some 1.5 GB above the loaded RSS.

FT.INFO's memory parts, MiB (v1.2.2 / candidate): TEXT terms 125.8 / 157.8,
TEXT postings 335.9 / 0.25, TEXT forward entries 54.9 / 54.9, key ids 188.9 /
122.4, TAG 282.5 / 198.6, NUMERIC 94.0 / 94.0. Most of the 2,000,001 terms
and 2,000,000 tags are in one document each: the documents of such a term or
tag are held in place now, not in lists (of capacity 4 for a term's fields)
of their own.

## Recorded run, suite 34

Recorded 2026-11-19 on an Azure Standard_D8s_v5 (Xeon Platinum 8370C),
`--only 34 --record`; the candidate is master `249f76eb` (this PR adds only the
suite), `cargo build --release --features lua,tls` by rustc 1.100.0 (the
compiler of the v1.2.2 release binaries, which are the `full` build here too);
memtier_benchmark 2.6.1; Redis 8.10.2 from the VM image. Runs:
`pr-951-d8s_v5-r1` (recorded) and `pr-951-d8s_v5-r2` (the noise check: every
gate passed, host speed 0.99x). A run takes 3,510 s. The server has CPUs 0-3
(KiviDB `--threads 4`, Redis one thread) and memtier 4-7, the suite's own split.

Kong sends its script as `EVAL` with the script text (lua-resty-redis
`red:eval`), not by its SHA-1, so the suite measures both: `eval` is what Kong
does, `evalsha` what a client that caches the script would do. The script is
Kong 3.9.4's `usage()`, as it is. `eval` is 9 % slower than `evalsha` for the candidate with the key
untagged (3 % tagged, 5 % for v1.2.2) and 9 % for Redis.

Operations a second, recorded run / rerun, and the candidate's ratio to Redis in
the same session. `default` is `lua-parallel-scripts yes`, which is also what
`yes` set by hand gives; `serial` is `lua-parallel-scripts no`. `plain` is
Kong's key as it is, `tag` has the identifier under a hash tag.

| config | call | key | conns | v1.2.2 | candidate | Redis 8.10.2 | candidate / Redis | candidate / v1.2.2 |
|---|---|---|---:|---:|---:|---:|---:|---:|
| default | eval | plain | 200 | 145,966 / 144,167 | 195,384 / 192,917 | 110,299 / 110,455 | 1.77x / 1.75x | 1.34x / 1.34x |
| default | eval | plain | 500 | 144,961 / 142,708 | 197,557 / 195,546 | 111,209 / 108,801 | 1.78x / 1.80x | 1.36x / 1.37x |
| default | eval | tag | 200 | 236,123 / 260,205 | 295,022 / 291,084 | 110,837 / 109,932 | 2.66x / 2.65x | 1.25x / 1.12x |
| default | eval | tag | 500 | 245,229 / 252,859 | 293,956 / 294,773 | 109,869 / 107,935 | 2.68x / 2.73x | 1.20x / 1.17x |
| default | evalsha | plain | 200 | 154,272 / 151,656 | 214,454 / 213,963 | 121,170 / 120,070 | 1.77x / 1.78x | 1.39x / 1.41x |
| default | evalsha | plain | 500 | 151,512 / 150,185 | 215,431 / 214,924 | 122,009 / 119,321 | 1.77x / 1.80x | 1.42x / 1.43x |
| default | evalsha | tag | 200 | 266,521 / 267,731 | 304,623 / 301,953 | 120,857 / 120,728 | 2.52x / 2.50x | 1.14x / 1.13x |
| default | evalsha | tag | 500 | 270,589 / 269,061 | 307,333 / 300,376 | 122,303 / 118,419 | 2.51x / 2.54x | 1.14x / 1.12x |
| serial | eval | plain | 200 | 147,491 / 146,087 | 199,520 / 200,858 | 112,644 / 109,557 | 1.77x / 1.83x | 1.35x / 1.37x |
| serial | eval | plain | 500 | 146,716 / 143,164 | 201,847 / 203,569 | 113,382 / 108,361 | 1.78x / 1.88x | 1.38x / 1.42x |
| serial | eval | tag | 200 | 146,608 / 144,801 | 195,594 / 199,348 | 112,494 / 110,001 | 1.74x / 1.81x | 1.33x / 1.38x |
| serial | eval | tag | 500 | 146,283 / 143,633 | 201,204 / 202,797 | 111,122 / 108,938 | 1.81x / 1.86x | 1.38x / 1.41x |
| serial | evalsha | plain | 200 | 156,399 / 155,812 | 222,578 / 220,570 | 122,079 / 120,015 | 1.82x / 1.84x | 1.42x / 1.42x |
| serial | evalsha | plain | 500 | 156,056 / 152,188 | 224,883 / 225,832 | 124,882 / 118,002 | 1.80x / 1.91x | 1.44x / 1.48x |
| serial | evalsha | tag | 200 | 155,754 / 153,276 | 216,294 / 216,415 | 123,404 / 119,917 | 1.75x / 1.80x | 1.39x / 1.41x |
| serial | evalsha | tag | 500 | 156,716 / 153,223 | 221,394 / 223,335 | 124,635 / 118,533 | 1.78x / 1.88x | 1.41x / 1.46x |

Every row is over the floors the suite gates on (1.2x Redis at 200 connections,
1.3x at 500): the lowest ratio measured is 1.74x. Those floors are about 30 %
under what was measured; they are the ones proposed in #849, and were not
changed to fit.

Against Redis, v1.2.2 runs Kong's key as it is (`plain`) at 1.24-1.33x at 200
and 500 connections, `eval` and `evalsha`, in both configurations; the master
candidate at 1.75-1.91x. With the key tagged, `default` runs the script
beside other work under the tag's lock: 2.2-2.4x Redis for v1.2.2 and 2.5-2.7x
for the candidate. With `lua-parallel-scripts no` a tagged key is as slow as
an untagged one (1.74-1.91x for the candidate), as the setting says. So the
1.4x at 200 connections and 1.5x at 500 published for v1.2.2 on Kong's script
is not reproduced by v1.2.2 here with the key untagged (1.24-1.33x on this
split of 4 server CPUs and 4 client CPUs), and the candidate clears it
(1.75-1.91x) in every configuration. p99 (shown, not gated) with the key untagged is
1.4-1.7 ms for KiviDB at 200 connections and 3.1-4.0 ms at 500, against
Redis's 3.2-3.5 ms and 7.8-8.8 ms.

`keys_without_ttl` was 0 on all three engines in every pass. The `MONITOR`
check passed everywhere: 2,000 calls (4 connections x 500) per engine,
configuration, call and key form, 1,978 distinct keys in them, every call with
one key of the asked-for form in the KEYS position and `60` as its only ARGV,
and 2,000 `incr` calls inside the scripts, each on a key a call named; for
`eval`, the script text equal to Kong's. For example (the first with the key as
Kong builds it, the second tagged):

```
"EVALSHA" "6d1bd3dc..." "1" "ratelimit:6f9c5f5c-...:0d3c6e1a-...:c87421:1763571600000:minute" "60"
"EVALSHA" "6d1bd3dc..." "1" "ratelimit:6f9c5f5c-...:0d3c6e1a-...:{c773}:1763571600000:minute" "60"
```

KiviDB v1.2.2 writes a newline in `MONITOR`'s script text as `\x0a`, Redis and
the candidate as `\n`: the check reads both. A call to an unknown SHA-1 makes
memtier log error replies and the suite fails, which was checked by hand.

This suite declares `min_cpus=8`: run it on a Standard_D8s_v5 with
`run-all.sh --candidate <bin> --candidate-full <bin> --min-cpus-only --label
<label>-d8s_v5`, or `--only 34` on a machine with 8 CPUs.

## Recorded run, suite 35

Recorded 2026-11-19 on an Azure Standard_D4s_v5 (Xeon Platinum 8370C),
`--only 35 --record`; the candidate is #891 at `3747bb7b` (master `b701f59f`
merged in), `cargo build --release` by rustc 1.100.0. Runs:
`pr-891-d4s_v5` (recorded) and `pr-891-d4s_v5-rerun1`, `-rerun2` (the same
binaries again: `ok` twice). A run takes about 2 minutes.

| metric | v1.2.2 | candidate (recorded / rerun 1 / rerun 2) | gate |
|---|---:|---:|---|
| `SET` 1 KiB, pipeline 16, a second | 287,617 | 587,037 / 581,730 / 589,116 (2.0x) | rel 10%; keep 60% of gain |
| `SET` 4 KiB, pipeline 32, a second | 96,304 | 235,149 / 237,883 / 239,279 (2.4-2.5x) | rel 10%; keep 60% of gain |
| 19 MB pipeline through a 64 KiB receive buffer | stalls (0; given up after 20 s) | 1 (written in 0.13 s) | `>= 1` |

v1.2.2's rates moved by 3 % across the three runs (287,617 to 296,520 for
1 KiB), the candidate's by 1-2 %.

## Recorded run, suite 36

Recorded 2026-11-19 on an Azure Standard_D8s_v5 (Xeon Platinum 8370C),
`--only 36 --record`; the candidate is `2cb7377cf` (PR #962),
`cargo build --release --features lua,tls` by rustc 1.100.0; memtier_benchmark
2.6.1; Redis 8.10.2 from the VM image. Runs: `pr962-d8s_v5-r1` (recorded)
and `pr962-d8s_v5-r2` (the noise check: every gate passed, host speed 1.00x).
A run before them, not kept, measured the ratios the floors are set from
(`FLOORS` in the suite). A run takes 1,600 s. The server has CPUs 0-3
(KiviDB `--threads 4`, Redis one thread) and memtier 4-7.

Operations a second at 200 / 500 connections, recorded run, and the
candidate's ratio to Redis in the same session (the noise run within 2 % of
each candidate value):

| workload | v1.2.2 | candidate | Redis 8.10.2 | candidate / Redis | floor |
|---|---:|---:|---:|---:|---:|
| `rpm_key` (`BATCH_RATE_LIMITER`, one tag) | 181,858 / 184,200 | 224,614 / 225,923 | 84,888 / 81,477 | 2.65x / 2.77x | 1.8x |
| `rpm_key_team` (`BATCH_RATE_LIMITER`, two tags) | 60,975 / 59,788 | 167,294 / 166,639 | 67,136 / 64,231 | 2.49x / 2.59x | 1.7x |
| `tpm_key_team` (`CHECK_AND_INCREMENT_BY_N`) | 40,079 / 40,865 | 119,169 / 121,206 | 47,175 / 45,604 | 2.53x / 2.66x | 1.7x |
| `parallel_key_team` (`PARALLEL_ACQUIRE`) | 32,251 / 33,121 | 117,500 / 119,404 | 51,643 / 50,442 | 2.28x / 2.37x | 1.6x |
| `request_mix` (13 commands a request) | 75,363 / 73,614 | 232,043 / 234,294 | 93,134 / 88,008 | 2.49x / 2.66x | 1.7x |

p99 (shown, not gated), ms at 200 / 500 connections, candidate against
Redis: `rpm_key_team` 1.36 / 3.73 against 5.79 / 14.97, the mix 1.41 / 3.63
against 4.22 / 11.26; v1.2.2 runs a key + team call alone and its p99 for
the mix is 8.03 / 20.35.

With one tag, v1.2.2 already ran the script beside others (2.1-2.3x Redis);
with two (`*_key_team` and the mix), where it ran alone, it is 0.6-0.9x
Redis and the candidate 2.3-2.7x. Of the candidate's 4 to 10 million
scripts a row, all but at most 63 ran beside others (`script_counts` in
`raw.tar.gz`; the gate is 90 %), the transcript
matched on all three engines, `keys_without_ttl` was 0 everywhere, and the
MONITOR check found one `<n>` across each call's `api_key` and `team` keys.

This suite declares `min_cpus=8`: run it on a Standard_D8s_v5 with
`run-all.sh --candidate <bin> --candidate-full <bin> --min-cpus-only --label
<label>-d8s_v5`, or `--only 36` on a machine with 8 CPUs.

## Recorded run, suite 37

Recorded 2026-11-20 on an Azure Standard_D4s_v5 (Xeon Platinum 8573C),
`--only 37 --record`; the candidate is the #862 change (`abba1a73` on its
branch, on master `73bb6fba`), the default build, rustc 1.100.0. Runs:
`pr-862-d4s_v5` (recorded), `pr-862-d4s_v5-rerun` (the noise check, with
the gates as they are now: every gate passed) and
`master-73bb6fba-d4s_v5-37` (master itself as the candidate, the
candidate engine only: the walk this replaces). A run takes about 2
minutes.

The server's own time a call (`INFO commandstats`), microseconds; rerun in
brackets:

1,000,000 members:

| command | v1.2.2 | master | candidate | Redis 8.10.2 |
|---|---:|---:|---:|---:|
| `ZRANDMEMBER z` | 4,641 | 1,047 | 1.02 (0.47) | 1.13 (1.15) |
| `ZRANDMEMBER z 1` | 5,147 | 1,124 | 1.11 (0.93) | 1.21 (1.18) |
| `ZRANDMEMBER z 10` | 5,316 | 1,975 | 3.18 (2.35) | 8.24 (8.49) |
| `ZRANDMEMBER z 10 WITHSCORES` | 5,554 | 1,981 | 3.94 (2.91) | 8.57 (8.65) |
| `ZRANDMEMBER z 1000` | 5,731 | 2,454 | 159 (163) | 711 (683) |
| `ZRANDMEMBER z 1000 WITHSCORES` | 6,857 | 2,680 | 216 (224) | 740 (768) |
| `ZRANDMEMBER z -10` | 4,490 | 1,994 | 3.15 (4.05) | 7.29 (7.51) |
| `ZRANDMEMBER z -1000` | 5,101 | 2,444 | 243 (253) | 485 (531) |
| `ZRANDMEMBER z -1000 WITHSCORES` | 5,723 | 2,662 | 328 (353) | 652 (652) |

100,000 members:

| command | v1.2.2 | master | candidate | Redis 8.10.2 |
|---|---:|---:|---:|---:|
| `ZRANDMEMBER z` | 231 | 70.3 | 0.06 (0.31) | 2.02 (2.11) |
| `ZRANDMEMBER z 10` | 264 | 143 | 1.29 (3.38) | 6.58 (6.88) |
| `ZRANDMEMBER z 1000` | 399 | 345 | 86.9 (90.0) | 474 (513) |
| `ZRANDMEMBER z -1000` | 377 | 333 | 128 (146) | 341 (377) |

At 1,000 members (a table of a thousand: the walk was short) every row is
within a few microseconds on all four; a count of 1000 there is the whole
set. The client's wall time a call (`<case>_us`) is the server's plus about
50 us of round trip and redis-py's parsing, about 0.7 ms for a thousand
members: at 1,000,000 members `ZRANDMEMBER z 10` takes 66 us (57) against
master's 2,040 and Redis's 61 (60). `bad_replies` is 0 on every engine.

## Not covered

Every row of the gains table in `docs/PERFORMANCE.md` has a suite here (01-13; 14 is #759 and 15 is #758, after that table).
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
