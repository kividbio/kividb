# KiviDB 1.2.0-rc8: failover subset

**Build under test:** commit `082dab3d8929c180bccf993835e28992ea054fb5` on `release/1.2.0` of kividbio/kividb (not
tagged). `Cargo.toml` says `1.2.0-rc8`, and `kividb --version` prints `kividb 1.2.0-rc8`. It was built from source on
the build VM with `cargo build --release` (default features, so the default variant), rustc/cargo 1.99.1 stable, in
4 min 16 s.

**SHA-256 of `target/release/kividb`:** `a9ea83cbb399ec8e921442caf59ea1a9a4fa8f2e65f561366cddd7d268299cd2`. The same
binary was copied to every VM, and the hash was checked on each one.

This is rc8 only, with 2 runs of each check unless noted. rc7's numbers come from `rc7tests/failover/README.md`.

## Verdict

**Every check passes. No acknowledged write was lost anywhere a loss isn't allowed. No crash, and every server
started by itself.**

| # | Check | Result | Key numbers |
|---|---|---|---|
| 1 | kill -9 standalone (everysec and always), kill -9 at checkpoint start, SIGSTOP standalone and cluster, partition, CLUSTER FAILOVER, cluster kill -9 | **PASS** | 16 runs, 11.24 M acknowledged writes, **0 lost, 0 wrong values**. 6 kills at checkpoint start. 0 replica differences in the 10 standalone runs. Async-window row: **0** (rc7: 1 in 3) |
| 2 | 2 power-offs, AOF on, `aof-ack-after-write yes` | **PASS** | **Started by itself 2/2.** Rep 1: AOF ended in 396 zero bytes, cut with the WARN; 1,329 of 3,402,046 lost, all acknowledged within the last **0.121 s**, each writer's last writes. Rep 2: clean AOF, **0 lost**. 0 wrong values |
| 3 | Expire-replay repro (`expirefix`) | **PASS** | 2/2 for each of the 3 scripts. Keys and fields that expired while the server was down stay gone after 1 and 2 restarts. The control key keeps its TTL. 4 `UNLINK`s in the AOF. Same as rc7 |
| 4 | **#303:** two engines in one data directory, under write load, AOF on | **PASS** | **8/8.** SIGTERM 0/200/600 ms after B starts, ×2: B waited 0.2-0.8 s for the lock, then loaded everything: **0 lost, 0 wrong** of 89,747-97,281 acknowledged, plus all 300,000 old keys. "First never stops", ×2: after 30 s B was still waiting, not listening, with only `kividb.lock` open in A's directory. A meanwhile acknowledged ~505 k writes with 0 lost. After A's SIGTERM, B (waited 42.8-42.9 s) held all of them |
| 5 | MIGRATE with `--io-model tokio`, and MIGRATE stopped by TRYAGAIN | **PASS** | Failpoint probe: **0/10** stalls at 0.2 s and **0/10** sent at once, ×2. Slow destination: **0/10** stalled trials, ×2. TRYAGAIN: **12/12** (tokio and uring × no auth/AUTH/AUTH2, ×2). The reply says the copies "were deleted there", the destination no longer holds the key, a **second MIGRATE without REPLACE answers OK**, and the destination then holds the new value |

**Regressions against rc7:** none found. Rates and outage times match rc7 to within 0.3 s (table below).

## Set-up

- **Where:** RG `kividb-rc8-failover`, region **japaneast** (it accepted every VM first time, so canadacentral was
  not needed).
- **VMs:** Ubuntu 24.04 LTS, one proximity placement group, accelerated networking, auto-shutdown on every VM.

| VMs | Size | Role |
|---|---|---|
| `fo1`-`fo3`, client `foc` | Standard_D4s_v5 (4 vCPU, 16 GB) | failover set A: rep 1 of each scenario |
| `fo4`-`fo6`, client `fod` | the same | failover set B: rep 2 (in parallel with set A) |
| `pf1`, client `pfc` | the same | power-offs. Data on the Premium SSD OS disk (`/data`), as in rc7 |
| `bld` | Standard_D16s_v5 | the build, then items 3, 4 and 5 (single host) |

- **Clients:** Python 3.12, redis-py 5.2.1 with hiredis 3.0.0, Ubuntu's `redis-cli` 7.0.
- **Data directories:** rc8 takes `kividb.lock` in its data directory. Every server in these harnesses already runs
  in a directory of its own (`/data/<port>` in `node.sh`, `/tmp/...` per node in the probes), so nothing needed to
  change. In #303's test, sharing a directory is the point.

## 1. Failover scenarios (rc8, 2 runs each)

The harness is the rc7 one (`benchmarks/1.2.0/failover`, unchanged between rc7 and 082dab3d, plus rc7's
`fo_run.sh`/`ackcheck.py`). It uses 16 writers on unique keys, and every acknowledged `SET` is read back at the end.
The standalone runs are a master + replica; the cluster runs are 3 masters + 3 replicas with
`cluster-node-timeout 5000`.

| Scenario | Runs | Acked writes | Lost acked | Wrong | Writes unavailable (s): rc8 | rc7 | Replica differs | Verdict |
|---|---|---|---|---|---|---|---|---|
| kill -9 master, AOF everysec, restart | 2 | 637,574 (636,825-638,323) | **0** | 0 | 5.0 (4.9-5.0) | 5.3 | 0 | pass |
| kill -9 master, appendfsync always, restart | 2 | 632,326 (632,099-632,554) | **0** | 0 | 5.2 (5.2-5.3) | 5.3 | 0 | pass |
| kill -9 master as an AOF checkpoint starts, 3 times | 2 | 860,711 (858,438-862,984) | **0** | 0 | 4.3 | 4.3 | 0 | pass |
| SIGSTOP master 30 s | 2 | 579,268 (575,963-582,573) | **0** | 0 | 30.0 | 30.0 | 0 | pass |
| Partition master <-> replica 30 s (no promotion) | 2 | 925,030 (923,565-926,496) | **0** | 0 | 0.0 | 0.0 | 0 | pass |
| Cluster: CLUSTER FAILOVER | 2 | 605,356 (604,758-605,953) | **0** | 0 | 0.1 | 0.1 | – | pass |
| Cluster: kill -9 a master | 2 | 643,736 (642,499-644,974) | **0** | 0 | 6.2 (6.1-6.2) | 5.9 | – | pass |
| Cluster: SIGSTOP a master 30 s (writes the replica had received) | 2 | 733,508 (733,306-733,710) | **0** | 0 | 7.3 | 7.3 | – | pass |
| ↳ of which promoted past unreplicated writes (async window) | 2 | – | **0** | – | – | – | – | none this time (rc7: 1 write in 1 of 3 runs) |

Medians, with the range in brackets. Per-run rows are in `results/summary.csv`, the generated table in
`results/table.md`, and the charts in `charts/`.

**Cluster SIGSTOP in detail (the async-window row).** Offsets are from the servers' logs (`results/runs/kividb/c_sigstop/<rep>/logs-*.tgz`):

| Rep | Replica promoted at | Old master's stream ended at | Stream past the replica | Acked writes lost |
|---|---|---|---|---|
| 1 | 5,231,471 | 5,232,193 | 722 bytes | 0 |
| 2 | 5,213,127 | 5,213,851 | 724 bytes | 0 |

In both runs the stream the replica missed held only writes that had not been acknowledged yet, as in 17 of rc7's 18
instrumented stops. The window rc7 measured (2 of 21 stops lost one write each) is a property of asynchronous
replication, and 2 runs here are too few to say anything about its rate.

## 2. Power-offs under write load (AOF on, `aof-ack-after-write yes`)

**The load:** 16 writers against `pf1`, with `appendonly yes`, `appendfsync everysec` and `aof-ack-after-write yes`.
The last of these is the default, set explicitly here in `fo_run.sh`. That is about 11,300 acknowledged SETs a
second.

**The cut:** at t ≈ 31 s, `az vm stop --skip-shutdown` from the operator's machine (no OS shutdown), then
`az vm start`. The driver is rc7's fixed `pl_drive.sh`; it saw no SSH failures this time. The server was restarted
from disk with nothing cut by hand.

| Rep | Acked writes | Lost acked | Lost writes acknowledged within … before the cut | Lost acked after restart | Each writer's last writes only | Wrong | AOF before restart | Started by itself |
|---|---|---|---|---|---|---|---|---|
| 1 | 3,402,046 | 1,329 | **0.121 s** | 0 | yes | 0 | 28,456,795 bytes, **ending in 396 zero bytes**. `[WARN] AOF ended with a partial record … the incomplete tail (396 bytes) is cut off` | **yes** |
| 2 | 3,382,614 | **0** | – | 0 | yes | 0 | 30,093,215 bytes, 0 trailing zeros | **yes** |

- Both servers started by themselves, including the one whose AOF ended in zeros.
- What was lost is the documented everysec window: a power cut can lose up to a second of writes that are in the
  page cache but not yet fsynced. Here it was ≤ 0.121 s.
- Nothing older than a writer's last writes went missing, nothing was lost after the restart, and nothing came back
  with another value.
- rc7: 5/5 started; lost ≤ 0.256 s.

Raw data is in `results/power-off/<rep>/` (`summary.json`, `timeline.csv`, `events`, `lost.json.gz`,
`aof-before-restart.txt`, the server log), with the per-run analysis in `results/power-off/pl-analysis.jsonl`.

## 3. Expire replay (`expirefix` repro)

`expire_replay.sh`, `field_replay.sh` and `expire_more.sh` are rc7's copies, run 2 times each on rc8 with
`appendfsync always` (`results/bld/expire/`, summary in `results/bld/summary.txt`).

| Script | After the restart(s), both runs |
|---|---|
| `expire_replay.sh` | h, s, l gone; DBSIZE 0 |
| `field_replay.sh` | `g` gone; `f` kept with no TTL; HLEN 1 |
| `expire_more.sh` | restart 1 and restart 2: x1-x4 gone, both fields gone, `hq` keeps `keep`, the control keeps its TTL (3,597 s, then 3,594 s) and 2 elements; **4** `UNLINK`s in the AOF |

This is identical to rc7, and to Redis 8.10.3 in the rc7 run.

## 4. #303: two engines in one data directory (new in rc8)

**Harness:** `harness/r303load.sh` (+ `r303fill.py`, `r303all.sh`). It follows `rc8fixes/scripts/repro303.sh` and adds
write load and the AOF.

**Engine A**, the master (port 7100):
- `appendonly yes`, `appendfsync everysec`, `aof-ack-after-write yes`.
- `save 3600 1`, as in the repro. rc8 logs that `save` has no effect; the dump comes from the explicit `SAVE`.
- 300,000 keys, then a `SAVE`, which leaves an old `dump.kdb`.
- 16 `ackcheck.py` writers then write unique keys to A for the whole run.
- A replica R follows A from its own directory.

At t = 8 s, **engine B** is started in A's data directory (port 7101).

**SIGTERM to A, 0 / 200 / 600 ms after B starts**, as a kubelet does after a forced delete. The writers keep going
throughout. Every write A acknowledged is then read back from B. Then R is pointed at B and must end with B's dataset.

| Delay | Rep | Acked by A | Lost | Wrong | B's keys | A exited after SIGTERM | B waited | B loaded | R after `REPLICAOF B` |
|---|---|---|---|---|---|---|---|---|---|
| 0 ms | 1 | 90,025 | **0** | 0 | 390,025 = 300,000 + acked | 0.21 s | yes, free after 0.2 s | 390,025 keys from the snapshot, then the AOF | 390,025 keys, partial resync |
| 200 ms | 1 | 91,739 | **0** | 0 | 391,739 = 300,000 + acked | 0.21 s | yes, 0.4 s | 391,739 | 391,739 keys, full sync |
| 600 ms | 1 | 96,754 | **0** | 0 | 396,754 = 300,000 + acked | 0.19 s | yes, 0.8 s | 396,754 | 396,754 keys, partial resync |
| 0 ms | 2 | 89,747 | **0** | 0 | 389,747 | 0.21 s | yes, 0.2 s | 389,747 | 389,747, partial |
| 200 ms | 2 | 91,957 | **0** | 0 | 391,957 | 0.21 s | yes, 0.4 s | 391,957 | 391,957, partial |
| 600 ms | 2 | 97,281 | **0** | 0 | 397,281 | 0.21 s | yes, 0.8 s | 397,281 | 397,281, partial |

**What B did, every time:**
- It logged `The data directory … is in use by another process (it holds kividb.lock): waiting for it to be free`,
  then `free after …s: starting`.
- It loaded the dump A wrote as it stopped, plus the AOF, and nothing older.
- It continued A's stream (`can be continued`). That is safe now, because the dataset is A's latest, and every key
  matches.
- **0 lost in 6 runs.** On rc7, `repro303.sh` lost 300,000 of 900,000 keys in every run (`rc8fixes/repro303-rc7.txt`).

**One R in six did a full sync rather than a partial one** (200 ms, rep 1). It still ended with exactly B's
391,739 keys. A full sync is the safe fallback, not a loss. I did not investigate it further.

**First engine never stops (×2).** B is started in A's directory under the same load, and A keeps running. B is
checked at t = 38 s, 30 s after it started.

| Rep | B alive | B listening on 7101 | `PING` to B | B's open files in A's directory | B's log | A meanwhile | Then SIGTERM to A |
|---|---|---|---|---|---|---|---|
| 1 | yes | no | connection refused | `kividb.lock` only | 6 lines `… in use by another process … waiting (25s so far)` | 504,611 acked, **0 lost**, 0 wrong | A exited in 0.47 s; B "free after 42.9 s", 804,611 keys, **0 lost** of 504,611 |
| 2 | yes | no | connection refused | `kividb.lock` only | the same | 504,785 acked, **0 lost** | 0.43 s; B free after 42.8 s, 804,785 keys, **0 lost** |

B waits and does nothing else: it binds no port and reads or writes no data file. A's files are left to A
(`a-dir-while-B-waits.txt`). A's write rate is unaffected: a median of 1,133 acknowledged SETs per 100 ms before B
started, against 1,125 while B waited (rep 1: 1,132.5 / 1,126; rep 2: 1,134 / 1,124).

Per-run data is in `results/bld/r303/<mode>-<delay>-<rep>/`: `verdict.txt`, the A/B/R logs, `events`,
`summary.json`, `lost.json`, `timeline.csv`. The one-line verdicts are in `results/bld/r303.txt`.

## 5. MIGRATE with `--io-model tokio`, and MIGRATE stopped by TRYAGAIN

**The repo's probe** (`benchmarks/1.2.0/fixes/migrate-tokio-stall/probe.py`): two nodes, `--io-model tokio --threads 2`,
with the `migrate-before-delete` failpoint holding MIGRATE. A `PING` goes out on a fresh connection, with a 3 s
timeout.

| MIGRATE sent | Rep 1 | Rep 2 | rc7 |
|---|---|---|---|
| 200 ms after connecting | **0/10** stalls | **0/10** | 0/10 |
| at once | **0/10** | **0/10** | 0/10 |

**A slow destination** (`migrate_slowdst.py`): a fake destination that answers nothing for 3 s. **0/10** stalled
trials in each rep (rc7: 0/10).

**MIGRATE stopped by TRYAGAIN** (`harness/migtry.sh`, after `rc8fixes/scripts/authcheck.sh`). Each node is a cluster
node in its own directory, and the slot is MIGRATING/IMPORTING. While MIGRATE is held at the failpoint, the key is
`INCR`ed at the source. This runs on tokio and uring, each with no auth, `AUTH` and `AUTH2` at the destination, 2 runs
each: **12/12 PASS**. Every run gave the same result:

- MIGRATE's reply: `TRYAGAIN a key was written while being migrated, and kept here: 'k:2'. The copies sent to
  127.0.0.1:7601 were deleted there; MIGRATE them again`;
- the source holds the new value (2);
- **the destination no longer holds the key** (`ASKING; EXISTS` = 0);
- **a second MIGRATE without REPLACE answers `OK`**;
- the destination then holds `2`, and the source answers `-ASK 6101 127.0.0.1:7601` for the key, so it is gone from
  the source.

On rc7 the destination kept the older copy it had been sent (commit 42a7282a, #295), so moving the key again needed
REPLACE.

Outputs are in `results/bld/migrate/`.

## Harness notes

- **First pass of items 4 and 5** (`results/bld/first-pass/`):
  - **#303:** in `bld8.sh`'s first pass, every run's verdict line was swallowed. Bash printed the `Killed` notice of
    the job after it, and `tail -1` took that. The raw data from that pass already showed 0 lost in all 8 runs.
    `r303load.sh` now also writes `verdict.txt`, and all 8 runs were done again (`r303all.sh`). The table above is
    from that second pass.
  - **TRYAGAIN:** `migtry.sh` first marked every run FAIL. It expected `EXISTS` at the source to answer 0 after the
    second MIGRATE. A migrating node answers `-ASK` for a key it no longer holds, which is correct. The check now
    accepts `0` or `-ASK`, and both reps were run again. All the other checks (TRYAGAIN text, the destination's copy
    gone, MIGRATE again OK, the destination's value) passed in the first pass too.
- `pl_drive.sh` is rc7's fixed copy (it retries while ssh exits 255). It needed no retries here.

## Reproduce

1. `harness/create.sh` creates the RG and the VMs. `harness/setup_all.sh` copies the harness and runs `base8.sh` per
   role. On `bld` it also runs `build8.sh`, which builds 082dab3d, records the SHA-256 and copies the binary to every
   VM, then starts `bld8.sh` (items 3-5).
2. **Failover:** run `bash ~/fo/harness/suite8.sh 1` on `foc` and `suite8.sh 2` on `fod`, with `hosts.env` holding
   each set's server IPs. Then merge the two `fo-results` trees and run `python fo_report.py <merged> <out>`.
3. **Power-offs:** from a machine with `az`, run `harness/pl2.sh` (it uses `s.sh` and a `vms.tsv` of name/public
   IP/private IP). Then run `python3 pl_analyze.py 1 2` in the client's `pl-results/kividb/power_loss_everysec`.
4. **#303 and TRYAGAIN alone:** `bash r303all.sh ~/out/r303` and `bash migtry.sh /opt/kv/kividb`.

## Teardown

- `bld`, `pf1` and `pfc` were deleted once their results were fetched. The failover sets went with the RG after their
  results were merged and fetched.
- Then `az group delete -n kividb-rc8-failover --yes`, and `az group exists -n kividb-rc8-failover` answered
  **false**.
