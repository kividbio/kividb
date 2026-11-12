# 1.2.1 functional release checks (35a413c0)

**Build under test:** 1.2.1 (35a413c0), built the way `release.yml` builds the Linux x86_64 release: an
`amazonlinux:2023` container, rustup stable (rustc 1.99.1), `cargo build --release` (fat LTO, codegen-units 1).
`--version` prints a pre-release string; expected, only the version string changes for the release.

| variant | sha256 | build time |
|---|---|---|
| default | `b7ebacb76961b4b8b52981728b4cddae9c65f17a71cbb2d0c022736afed86b0f` | 370 s |
| full (`lua,tls`) | `8e9225fe721029070ff4ababdb723144055464acc5f20fa1bf95518a63d21a32` | 391 s |

The same two binaries ran on every VM. The cargo integration tests on the expiry VM were built there from the same
tree (release, LTO off, `lua,tls`). **1.2.0:** the released `kividb-linux-x86_64.tar.gz` (and `-full-` for nothing
here), checked against `SHA256SUMS.txt` on every VM; binary sha256 `a211d544be79bb78…`.

**Comparison point:** the earlier final pass on 878746e4 / 5914e502 (expiry), same harnesses (`harness/<area>/`).

**Machines (Azure, Ubuntu 24.04, NSG: SSH from one /32):**
- centralindia (RG `kividb-c3-func`): `fp` D16ds_v5 + P30 1 TiB data disk (host caching None) for the build and
  persistence; `pcli` D4s_v5 (power-loss client, snapshot compatibility); `fe` D8s_v5 (expiry); `fs` D16s_v5 (Tcl
  suite; earlier pass: two D32s_v6).
- westus2 (RG `kividb-c3-func2`; eastus2, swedencentral, southindia and uksouth had no capacity or cores): cluster
  `cl-c1..c3` + `cl-cc`, replication `cl` + `n1..n3`, all D8s_v5. The replication client `cl` is a D8s_v5 (earlier
  D16s_v5), so acked-write counts there are ~15% lower (client-bound); that is not a server change.

## Verdict

**Everything passes. No new bug.** Two behaviour changes against the comparison point, both explained: `WAIT` without
replicas now waits out its timeout (documented change, see 4), and hash-field deadlines now come through every path
exactly (the 1 ms drift is gone). One open observation (4) and the known open issues listed below.

| # | Check | Result | vs comparison point |
|---|---|---|---|
| 1 | Expiry re-test (integration, Tcl, idle CPU, clock steps, oracle load, kill -9) | PASS | same or better |
| 2 | Redis Tcl suite, 41 units, per unit | PASS: uring 2696/6/0, tokio 2698/4/0 | identical per unit, same failing test names |
| 3 | Persistence (`appendfsync always`, kill -9, rewrites, power loss) | PASS: 188/188 checks, 0 lost | same; #489 drift gone |
| 4 | Replication, failover, Sentinel, field TTLs, mixed versions | PASS | WAIT recovery 1.0 s -> 6.8 s (documented) |
| 5 | Cluster 3+3 (#516 repro, failover during ASM, kill -9, reshard) | PASS: 0 acked writes lost in 36 runs (1 stale in the async window) | same |
| 6 | Snapshot loading across versions | PASS: 11 of 12 comparisons exact; 1 field deadline 2 ms early (1.2.0-written file) | new check |

## 1. Expiry re-test (`harness/expiry/`, fe D8s_v5)

| item | uring | tokio | earlier (5914e502) |
|---|---|---|---|
| integration tests (12 files, lua,tls) | 213 passed, 1 ignored | 213 passed, 1 ignored | 208 passed, 1 ignored |
| lib `store:: active_expiry expiry eviction ttl` | 112 passed | (in-process) | 110 passed |
| expiry_propagation x20 | 20/20 | 20/20 | 20/20 |
| Tcl unit/expire | 60/0 | 60/0 | 60/0 |
| Tcl unit/type/hash-field-expire | 32/0 | 32/0 | 32/0 |
| Tcl unit/maxmemory, integration/replication* | 0 run (external:skip), replication-buffer 4/0 | same | same |
| idle CPU, 5M EX3600 + 5M spread + 1M EX86400 | 1.28% | 1.42% | 1.53 / 1.72% |
| idle CPU, 5M EX 3600 alone / 5M spread alone / empty | 0.52 / 1.08 / 0.22% | 0.67 / 1.25 / 0.38% | 0.63/1.32/0.18, 0.70/1.40/0.33% |
| clock -1 h then back: early expiries; CPU back / after | 0; 0.93% / 1.58% | 0; 1.10% / 1.63% | 0; 1.0/1.8, 1.2/2.0% |
| clock +1 h then back: early expiries; c:* (EX 86400) kept; CPU after | 0; 1,000,000/1,000,000; 0.30% | 0; 1,000,000; 0.42% | 0; 1,000,000; 0.28 / 0.42% |
| load 5 min, oracle | 5.62 M ops, 0 errors | 4.57 M ops, 0 errors | 4.76 M / 4.48 M, 0 errors |
| replica quiet compare (keys, values, PEXPIRETIME) | 356,900 identical | 350,809 identical | 352,990 / 352,114 |
| replica compare under CLIENT PAUSE WRITE | **0 diverged** (3,118 only-master, all due) | **0** (3,074, all due) | 1 diverged (#628) |
| kill -9 x3 with AOF under load | 0 lost of 359,336 | 0 lost of 359,389 | 0 of 359,383 / 359,460 |

Idle CPU is a few tenths of a percent of one core, so the relative swings (e.g. empty 0.18 -> 0.22%) are noise; none
is a rise in absolute terms. The CLIENT PAUSE divergence of the earlier pass (#628, PERSIST on a due key) is gone.

## 2. Redis Tcl suite (`harness/redis-suite/`, fs D16s_v5)

redis 8.10.2 tests, `scripts/redis_compat.sh` from the tree, one unit per fresh server, full build, model forced and
checked (`I/O model : io_uring (forced)` / `tokio (forced)`). Nothing listening on 6399 beforehand.

| run | passed | failed | aborted |
|---|---:|---:|---:|
| earlier final, 878746e4, uring | 2696 | 6 | 0 |
| earlier final, 878746e4, tokio | 2698 | 4 | 0 |
| **35a413c0, uring** | **2696** | **6** | **0** |
| **35a413c0, tokio** | **2698** | **4** | **0** |

Per unit identical in all 41 units; the failing test names are exactly the earlier ones (no NEW, no GONE):
hyperloglog 20/1 (corrupted dense HLL, as 1.2.0), introspection-2 47/1 (as 1.2.0), latency-monitor 10/1
(environment: 1.2.0's binary fails it the same way on fast VMs), querybuf 1/2 on uring / 3/0 on tokio (as 1.2.0),
scripting 218/1 (Lua GC, known). `results/redis-suite-comparison.txt`.

## 3. Persistence (`harness/persistence/`, fp D16ds_v5 + P30)

**persist.py: 47 checks x 4 (default/full x uring/tokio) = 188/188 PASS**, 0 FAIL.

| check | default u | default t | full u | full t |
|---|---|---|---|---|
| `appendfsync always`: CONFIG GET = always, no "has no effect" warning | PASS | PASS | PASS | PASS |
| `always`, 16 writers, kill -9, 8 rounds | 55,946 acked, 0 lost | 49,236, 0 | 56,318, 0 | 49,274, 0 |
| `everysec`, same, 8 rounds | 275,346, 0 lost | 307,984, 0 | 311,201, 0 | 287,372, 0 |
| BGREWRITEAOF x5 under 16 writers, kill -9 (last one during a rewrite), `always` | 22,922, 0 lost | 23,353, 0 | 23,617, 0 | 30,221, 0 |
| same, `everysec` | 93,300, 0 lost | 108,724, 0 | 119,034, 0 | 130,422, 0 |
| BGSAVE under a writer: acked before BGSAVE present after kill -9 | 6,620 / 0 missing | 6,963 / 0 | 6,625 / 0 | 7,058 / 0 |
| SAVE / SHUTDOWN / BGSAVE / backup restore / `--preload-file rdb:` / #354 #368 / #531 field TTLs | PASS | PASS | PASS | PASS |

The earlier pass had `PASS*` (equal apart from relative field TTLs 1 ms early after AOF replay / backup restore,
#489). **None now**: every fingerprint is exact, and `fieldttl_drift.sh` gives 0 of 200 hashes for AOF replay,
backup restore and replica on both builds (earlier 185-188 of 200).

**Syncs per write** (bpftrace `sys_enter_fdatasync` on the server pid, redis-benchmark SET):

| build / io | always c=1 | always c=16 | everysec c=1 | everysec c=16 |
|---|---|---|---|---|
| default uring | 5,001 per 5,000 (1.00); 258 ops/s | 5,025 per 40,000 (0.126); 1,947 ops/s | 2 per 50,000; 28,393 | 4 per 300,000; 94,044 |
| default tokio | 5,000 (1.00); 228 | 5,045 (0.126); 1,758 | 2; 29,464 | 3; 133,630 |
| full uring | 5,000 (1.00); 262 | 5,032 (0.126); 1,872 | 2; 27,996 | 4; 91,855 |
| full tokio | 5,000 (1.00); 281 | 5,038 (0.126); 1,973 | 2; 29,499 | 3; 113,550 |

Same sync pattern as the earlier pass (one sync per acked write with one client, ~8 writes per sync with 16).
`always` throughput is about twice the earlier pass's (125-173 / 1,232-1,468 ops/s): it is bound by the disk's sync
latency, and this P30 in centralindia gives a 1-client p50 of 3.4-4.0 ms against 5.2-5.9 ms on the swedencentral one. The `everysec`
numbers are single functional runs, not a benchmark (full tokio c=16 113.6k vs 124.7k earlier; default tokio 133.6k vs
128.0k): both directions, within run-to-run noise of a one-shot redis-benchmark; the benchmark suites measure this.

**Power-loss-like test** (`powerloss_rounds.sh`: writers on pcli, `echo b > /proc/sysrq-trigger` on fp, new boot
time checked each round):

| mode | rounds | acked | lost |
|---|---|---|---|
| `always` (default uring x3, default tokio x2, full uring, full tokio) | 7 | 83,598 | **0** |
| `everysec` (default uring x2, default tokio) | 3 | 217,478 | 27,441 = 0.84-0.89 s of writes (documented "up to a second"; earlier 0.27-0.65 s) |

**Known, still present:** #545 (a snapshot temp file of a checkpoint cut by kill -9 is never removed; `tmpleft.sh`
re-run on 35a413c0, same result) and #519 (`DEBUG DIGEST-VALUE` of a hash with field TTLs changes across SAVE +
restart: `hdigest_repro.sh`, h2 and h4).

## 4. Replication (`harness/replication/`, westus2, cl + n1..n3 D8s_v5)

| # | Check | Runs | Acked | Lost acked | Unavailable (s) | Converged / digest | Earlier |
|---|---|---|---|---|---|---|---|
| 1a | kill -9 master, REPLICAOF NO ONE, old master back as replica | 3 | 1,755,905 | 0, 0, 0 | 0.7 | 3/3, 0 diffs | 0.7 s, 0 lost |
| 1b | same with `WAIT 1 200` after each SET | 3 | 769,859 (768,415 WAIT-acked) | **0 WAIT-acked**, 0 plain | **6.8** (recovery 6.9) | 3/3 | 1.0-1.1 s |
| 1c | same, `min-replicas-to-write 1`, `max-lag 2` | 3 | 1,568,514 | 0, 1, 0 (async window) | 7.2 | 3/3 | 7.2-7.3 s |
| 2a | Sentinel x3: kill -9 master | 3 | 2,045,894 | 1, 0, 1 (async window) | 6.2-6.3 | 6/6, digest equal | 6.3 s, 0 lost |
| 2b | same, `replica-priority 0` on n2 | 1 | 679,601 | 0 | 6.3 | n3 promoted | same |
| 3 | equality: streamed / full-synced / restarted replica, ~222k keys, every type | 3 (9 comparisons) | – | – | – | 0 real differences 9/9; type-aware digest equal 9/9 | same |
| 4 | field TTLs 1.2.1 -> 1.2.1: race 3 x 15 commands x 300; bulk lag_stop/lag_netem/fullsync x3, promote | – | – | – | – | **0 field/value differences, 0 HPEXPIRETIME differences** | 0 diffs; HPEXPIRETIME 1 ms on ~49% (#489) |
| 4 | #415 repro (`fexp_race_pair.py`), HPEXPIRE / HGETEX | 3 x 300 | – | – | – | 0,0,0 / 0,0,0 (1.2.0 control: 4 / 24) | 0 / 0 (control 3 / 28) |
| 5a-5c | mixed versions (1.2.0 master -> 1.2.1 replica, reverse, going back) | as before | – | – | – | as documented (same messages as the earlier pass) | same |
| 5d | rolling upgrade under load | 3 | 2,679,862 | 0 | 0.1 | 3/3 | same |

- **1b, WAIT: the longer unavailability is a documented 1.2.1 change, not a regression.** While the promoted node has
  no replica (21.0 s to 26.9 s after start), each writer's `WAIT 1 200` now waits out its 200 ms and answers 0
  (`wait_probe.sh`: 35a413c0 209 ms per SET+WAIT, 1.2.0 6 ms). CHANGELOG: "Without replicas, `WAIT n timeout`
  with `n` above 0 now waits out its timeout and answers 0, as Redis does; it answered at once." Writers resumed at
  21.03 s at one write per 203 ms each, and at full rate once the old master rejoined; the harness's "unavailable"
  measure counts that slow window. 0 WAIT-acked writes lost.
- Lost plain acks (1c: 1, Sentinel: 2) were all acked by the master within milliseconds before its kill -9: the
  asynchronous window, as in the earlier pass (4 of 4.95 M there).
- Acked counts are ~15% lower than earlier everywhere because the client VM is a D8s_v5 (was D16s_v5).
- **Field TTLs:** besides 0 differences, `HPEXPIRETIME` on the replica now equals the master's to the ms for every
  relative form (earlier ~880 of 1,800 at -1 ms, #489). The 1.2.0 control still diverges (lag_stop: 1,180 hashes).
- **Open observation (not reproduced, no draft):** in one of 9 replica "hide" checks during the netem lag, one field
  (of 11,189 read) was in `HGETALL` but `HEXISTS` said 0 inside the same `MULTI`. Earlier pass: 0 in 9. Targeted
  probes (`multi_clock.py` on a master, `multi_clock_replica.py` on a replica whose master is stopped at the
  deadline, 80 tries, both builds) gave 0 mixed answers. Possibly a field whose deadline fell inside the transaction
  on a lagging replica; worth one look by the coordinator, not a release blocker on this evidence.

## 5. Cluster 3 masters + 3 replicas (`harness/cluster/`, westus2, 4 x D8s_v5)

Driver `drive35.sh` (same phase scripts as the earlier pass; `p1_create.sh` prefills with `bg.py` instead of
redis-benchmark; no 1.2.0 control this time, the earlier pass has it).

| Check | Runs | Acked | Lost / stale / wrong / list_lost | Notes | Earlier |
|---|---|---|---|---|---|
| 1a. #516 local repro (IMPORT 0-1199 + CLUSTER FAILOVER of the source's replica 0-200 ms in) | 12 | 767,338 | **0 in all 12** | import 170-179 ms, all completed; fenced first attempts retried | 0 in 12 |
| 1b. #516 on the VMs, p6b (failover 0-500 ms after IMPORT) | 12 | 1,316,211 | **0** | 8 fenced + retried (whole import 1.4-1.7 s), 4 sent after the handover refused | 0 in 32 |
| 1b. p6c (failover 330-390 ms after IMPORT) | 8 | 1,377,379 | **0** | imports took 300-351 ms here, so all 8 landed 18-64 ms after the handover and were refused (by design) | p6c hit the last ms before handover in 6 of 8 |
| 2a. kill -9 of a master under load | 2 | 231,470 + 235,930 | 0 / 0 / 0 / 0 | promoted 5.70 / 5.80 s, rejoined 0.83 / 1.25 s | 5.28 / 6.09 s |
| 2b. kill -9 of the ASM source during IMPORT 5461-8460 | 1 | 212,628 | 0 / **1** / 0 / 0 | the stale write is the async window (below); 21,184 keys of the moved slots stay on the promoted replica (#517) | 0 / 0, no orphans that time |
| 2c. orphaned keys (`repro_orphan.py`) | 4 | – | – | kill at completion: 10,983 of 10,983 kept 3/3; 300 ms later: 0 | same (#517 open) |
| 3. `redis-cli --cluster reshard` 1200 slots | 1 | 316,654 | 0 / 0 / 0 / 0 | 50.3 s, rc 0 | 57.6 s |
| 4. end state | 1 | 24 runs re-verified | **0 in all** | `--cluster check` OK; replicas = masters (offsets, keys, 0 DEBUG DIGEST-VALUE diffs); 0 panics / `[ERROR]` | same |

- **2b stale = asynchronous window, not a loss in the migration.** The key is in slot 9900 (not a moving slot) of the
  killed source; its last ack was at 21.155 s, and the writers' first connection errors to that node are at
  21.187 s: the kill (sent over ssh, logged at 20.603 s) landed ~0.58 s after it was logged, ~30 ms after that
  ack. A write acked ~30 ms before a master's kill -9 that its replica had not yet received, then the replica
  promoted: Redis's asynchronous replication, as 1a/1c in replication. The end-of-run re-verify shows 0.
- Coverage gap vs the earlier pass: no failover was sent in the last ~30 ms before a handover this time (imports ran
  ~300 ms, p6c was tuned for ~380 ms). The 12 local repro runs and 8 fenced VM runs cover the import's middle.

## 6. Snapshot loading across versions (`harness/snapshot/`, pcli)

Dataset (`snapdata.py`): dbs 0, 5 and 9; strings (int, empty value, all 256 byte values, 1 MB, absolute and relative
TTLs), hashes (1,000 fields, empty field name, field TTLs by HPEXPIREAT and HPEXPIRE), lists (100,000 elements,
empty elements), sets (intset, 2,000, empty member), zsets (1,000, +inf/-inf, empty member), streams (2 groups, PEL,
consumers, XDEL, MAXLEN), JSON, time series (labels, retention, compaction rule), vector sets (attributes), HLL,
bitmap, geo, FT index over 200 hashes. ~3,300 keys. Fingerprint: every key's type, PEXPIRETIME, full value (hash
fields with HPEXPIRETIME, stream entries/groups/PEL/consumers, TS samples/labels/rules, vector elements with VEMB
and attributes), DBSIZE per db, FT index num_docs.

| scenario | result |
|---|---|
| S1 1.2.0 dump.kdb, no empty key -> 1.2.1 loads | **equal** (3,302 keys); 1.2.1 SAVE + restart: equal |
| S2 1.2.0 dump.kdb with `""` in db0 (string) and db5 (hash with a field TTL) -> 1.2.1 loads | **equal** (3,304 keys, both empty keys); log: `[WARN] snapshot was written by KiviDB 1.2.0 with the empty key in database(s) [0, 5], where 1.2.0 ends a database (#441): read as the empty key, the one reading of 5 that loads it`; SAVE + restart equal |
| S2 control: 1.2.0 on its own file with `""` | refuses: `[FATAL] dump.kdb cannot be loaded: unknown type tag 58` (another run: `bad key utf8`), `Refusing to start with an empty dataset...` (1.2.0's #441) |
| S3 1.2.1 dump.kdb (`""` in db0/db5, non-UTF-8 keys) -> 1.2.1 | **equal** (3,322 keys), field deadlines exact |
| S3 going back: 1.2.0 on that file | refuses (documented): `[FATAL] dump.kdb cannot be loaded: unexpected end of file` |
| S4a 1.2.0 AOF: snapshot (BGREWRITEAOF) + log (the other half, `""` in db5 only in the log, mutations), kill -9 -> 1.2.1 | **equal** (3,301 keys): `Loaded 1782 keys from snapshot`, `AOF replay: 3200 commands` |
| S4b same, SHUTDOWN (all in dump.kdb, 16-byte log) -> 1.2.1 | equal except one hash field deadline **2 ms early** (`d5:h:fttl` field b, a relative HPEXPIRE) |
| S5 full sync 1.2.0 master -> 1.2.1 replica (`""` in db0/db5, FT index) | **equal** after the sync, equal after streamed mutations, equal after the replica's own SAVE + restart |
| S6 control: 1.2.1 text-only dump.kdb -> 1.2.0 | equal (text-only data is the format 1.2.0 writes) |

No refusal by 1.2.1 anywhere. Two known differences are counted apart in every comparison: the order of a time
series' labels in `TS.INFO` (#612, open; also 1.2.1 -> 1.2.1) and **hash-field deadlines in 1.2.0-written files**:
on every load of a 1.2.0 file (by 1.2.0 itself too) field deadlines come back 1 ms early (once, S4b, 2 ms), never
late; 1.2.1's own files keep them exactly (S3), and 1.2.0 reading a 1.2.1 file shows +1 ms (S6). That is 1.2.0's
ms/clock conversion on both its write and read side (fixed in 1.2.1 by #594, closed); 1.2.1 cannot recover the ms a
1.2.0 writer dropped. Harmless (a field expires 1-2 ms early) and not new; no draft filed.

Also seen: KiviDB's AOF is `KIVIDLT1` header + RESP commands, on top of `dump.kdb` + `dump.kdb.gen` (the "preamble"
is the checkpoint snapshot; there is no RDB inside `appendonly.aof`), as docs/PERSISTENCE.md says.

## Known open issues seen (not re-filed)
#517 (orphaned keys after kill -9 of an ASM source at the handover), #519 (DEBUG DIGEST-VALUE of hashes with field
TTLs after restart), #545 (stale snapshot temp file), #612 (TS label order).

## Files
- `harness/`: `create1.sh`..`create4.sh` (VMs), `build_release.sh`, `ssh.sh`, `scp.sh`, `status.sh`; per area the
  earlier harness plus: expiry `setup.sh` (no server build); persistence data-disk fix, `plrun.sh`; replication
  `setup.sh` (no build), `wait_probe.sh`, `multi_clock*.py/sh`; cluster `setup.sh`, `drive35.sh`, `p1_create.sh`
  (bg.py prefill); snapshot `snapdata.py`, `snapcompat.sh`, `setup.sh`.
- `results/`: `raw.tar.gz` (everything: per-area logs, JSON, per-unit Tcl logs, fingerprints and diffs; unpack with
  `tar xzf raw.tar.gz`), plus loose summaries: `redis-suite-comparison.txt`, `replication-summary.csv`,
  `cluster-runtable.md`, `cluster-repro-local.txt`, `snapcompat.log`, `expiry-summary-tests.txt`,
  `persistence-drift-digest.txt`.
- `NOTES.md`: progress log.

## Teardown
`az group delete` of `kividb-c3-func` and `kividb-c3-func2`; `az group exists` -> `false` for both.
