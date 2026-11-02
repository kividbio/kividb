# `a_write_while_migrate_is_on_the_wire_is_not_lost` flake (PR #289, 2026-11-02)

## Cause

It is not a KiviDB lock. It is the tokio runtime's I/O driver, left stuck behind a MIGRATE that blocks a worker thread.

- **MIGRATE holds nothing at the failpoint.** It is not `gated`, so it takes no write gate and no write order: `handler.rs` sets `gated = … && name != "MIGRATE"`, and `ordered = gated && …`. It also skips the command gate (`command_gate::skips` lists MIGRATE). The order of its keys is taken only inside `delete_moved`, after the failpoint. A plain `PING` hangs too (see the probe below), so no per-key lock is involved.
- **Test nodes run `--io-model tokio --threads 2`.** Commands run inline on the two tokio workers. MIGRATE then blocks its worker in `std::thread::sleep` (the failpoint), or, in production, on its blocking socket to the destination.
- **tokio 1.50 multi-thread:** only one idle worker at a time polls epoll and the timers (the "driver").
  - A worker woken *by the driver* with a single task to run does not wake another worker: `should_notify_others()` needs more than one task (`worker.rs:1133`).
  - The other worker sleeps in `park_condvar`, and takes over the driver only once something wakes it.
  - The accept loop runs in `block_on` on the main thread, with a 50 ms interval, and needs the driver too.
  - So when MIGRATE's connection task is woken by the driver, the worker that holds the driver runs MIGRATE and blocks. Nothing polls epoll after that: no accept, no read, no timer.
  - PEXPIRE's connection sits in the accept queue. MIGRATE never gets the failpoint file removed, and both client reads time out after 10 s (EAGAIN, os error 11).
- **Why it is intermittent:** it happens only when MIGRATE's bytes arrive *after* the server's first read on the new connection found nothing (`Pending`). The test sends MIGRATE right after connecting, so the bytes are usually there already. A loaded runner delays the client between connect and write. CI's Linux Verification runs the 49 cluster tests 4-wide on a 4-vCPU runner.
- **Why it hit at 2 and not at 1:** the same coin is tossed at both hits. In our stressed runs, 23 of 24 hangs were at hit 2 (PEXPIRE) and 1 was at hit 1 (INCR), matching CI.

### Evidence

- **Probe** (`probe.py`): two standalone nodes, the failpoint on, then a `PING` from a fresh connection while MIGRATE waits.

  | Build | MIGRATE sent at once | MIGRATE sent 200 ms after connecting |
  |---|---|---|
  | release (664ecd59) | 0/10 stalled idle, 1/20 under stress | **10/10** stalled |
  | rc4 | | 5/5 stalled |
  | pre-#271 | | 5/5 stalled |
  | fixed | | **0/10** stalled |

- **Thread dumps:** 12 were taken from real hangs of the test (`logs/dumps/*.txt`, gdb `thread apply all bt` plus `ss`), and one from the probe (`logs/probe-rel-stall.txt`). Every one shows the same thing:
  - one `tokio-rt-worker` in `kividb::aof::failpoint` ← `commands::cluster::migrate` ← `server::handle_client`;
  - the other `tokio-rt-worker` in `multi_thread::park::Inner::park_condvar` (not `park_driver`);
  - the main thread in `CachedParkThread::block_on` park (the accept loop);
  - no thread of the main runtime in `epoll_wait`;
  - the client port's LISTEN Recv-Q at 1 (PEXPIRE's connection never accepted), and unread bytes on established connections.

So the answer is a mix of (i) and (ii): the test's assumption is fine in KiviDB's own locking, and the hang is a real runtime stall.

## Failure rates (`cargo test --test cluster a_write_while_migrate_is_on_the_wire_is_not_lost`, debug)

Azure D16s_v5, koreacentral. "Stress" is `stress-ng --cpu 16`, with the three builds' loops running at the same time.

| Build | Idle | Under stress |
|---|---|---|
| (a) release/1.2.0 664ecd59 | 0/100 | **6/100** |
| (b) v1.2.0-rc4 a6b1bf38 | 0/100 | **10/100** |
| (c) 2f8f15f5 (just before #271, PR #286) | not run | **8/100** |
| release + test change only (MIGRATE sent 100 ms after connect) | | **20/20** (all hang at hit 1) |
| release + fix + test change | | **0/100** |

## Regression?

No. (a), (b) and (c) fail at the same rate within noise. The #271 write-order stripes, the write gate, `pause::HANDOFF` and the release-only changes (`aof::stream_applied`, the 64 MB `write_queued` pass, gossip relayed addresses, the AOF zero tail, LF, the version) play no part.

The stall is as old as MIGRATE running inline on a tokio worker. The test that exposes it is new in rc4's cycle: #274 / PR #280 added it.

## Should it block 1.2.0?

Not on correctness:
- no write is lost, and nothing deadlocks for good;
- the stall ends when MIGRATE does;
- io_uring, the Linux default, is not affected this way: each uring worker is its own current-thread runtime and its own ring.

In production with `--io-model tokio` (macOS/Windows, or chosen), a MIGRATE that arrives on an idle connection can freeze the node's I/O for as long as it waits on the destination: client accepts and reads, the replica link, and the main runtime's timers. `redis-cli --cluster reshard` sends exactly such MIGRATEs. That is normally milliseconds, but can last up to the MIGRATE timeout if the destination is slow. Redis blocks its whole server during MIGRATE too, so this is parity rather than a new hazard.

Left as it is, the test will keep failing CI about 1 run in 10 under load. The fix below is small, so taking it in 1.2.0 is cheap. Otherwise, list it as a known flaky test.

## Proposed patch: `migrate-off-the-runtime.patch`

Not committed. It applies with `patch -p1` on 664ecd59.

- `src/commands/cluster.rs`: `handle_migrate` runs `migrate` in `off_the_runtime(...)`. On a multi-thread runtime that is `tokio::task::block_in_place`, which hands the worker's core, and the driver duty with it, to another thread before blocking. On a current-thread runtime (the io_uring workers via tokio-uring, the side runtimes) or with no runtime at all, it runs `f` directly; `block_in_place` would panic on a current-thread runtime. The closure runs on the same OS thread, so the thread-local gates are unaffected.
- `tests/cluster.rs`: `migrate_with_writes_meanwhile` connects, waits 100 ms, then sends MIGRATE. This makes the bad interleaving certain: without the fix the test fails 20/20, with it it passes 100/100 under stress. No new test function is needed; the existing test is now a deterministic regression test.

Checks on the patched tree:
- `cargo fmt --check`: OK.
- `cargo test --test cluster --test commands --test string-json -- --test-threads=4`, default features: OK. These are the files that use MIGRATE.
- MIGRATE under `--io-model uring` (`uring-check.py`): works, no panic.
- `cargo clippy --all-targets -- -D warnings`, both default and `--features lua,tls`: OK (latest stable).
- The same three test files with `--features lua,tls`: OK (`logs/checks.txt`).
- The final patch applies cleanly to a fresh 664ecd59 tree and passes `cargo fmt --check`. Clippy and the tests ran before a comment-only edit to the patch.

## Files

- `migrate-off-the-runtime.patch` (= `fix-product.patch` + `fix-test.patch`)
- `probe.py`, `loop.sh`, `watch.sh`, `uring-check.py`, `build.sh`, `checks.sh`: the scripts used on the VM.
- `logs/ci-linux-verification-attempt1.log`: the CI failure (run 41378870016, attempt 1).
- `logs/results/<build>-<idle|stress>/`: `progress.txt` (run, passes, failures, seconds) and the log of each failed run.
- `logs/dumps/`: thread dumps taken by `watch.sh` during real test hangs (the `exe:` line names the build).
- `logs/pdumps/`, `logs/probe-rel-stall.txt`: probe outputs and the probe's stall dump.
