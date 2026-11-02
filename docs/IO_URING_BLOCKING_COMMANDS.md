# Blocking commands under `--io-model uring`

**Status:** fixed. Kept as the record of what the bug was, why it was invisible
for so long, and what the fix costs — the same trap is easy to reintroduce.

**Found** by the e2e tests' first run against io_uring in CI (Linux), and
**fixed** by moving the wait out of synchronous dispatch and into the
connection loop. See `src/commands/blocking.rs`.

## The symptom

Against a server started with `--io-model uring`, every blocking command killed
the connection:

```
redis.exceptions.ConnectionError: Connection closed by server.
```

with, in the server log:

```
thread '<unnamed>' panicked at src/commands/zset.rs:2426:18
```

The tokio path (`--io-model tokio`) was unaffected. (The default, `--io-model
auto`, uses io_uring where the kernel allows it and tokio otherwise.)

## Root cause

The blocking commands waited by calling `tokio::task::block_in_place` and then
`Handle::current().block_on(...)` from the *synchronous* command-dispatch path:

```rust
// src/commands/zset.rs, before the fix
// block_in_place yields the tokio thread for other tasks while we wait.
// This is the same pattern used by BLPOP — safe with multi-thread runtime.
let result = tokio::task::block_in_place(|| { ... });
```

The comment states the constraint that was being violated: `block_in_place`
**panics on a current-thread runtime**, and `tokio_uring` is inherently
thread-per-core current-thread. The assumption held for `server.rs`
(multi-thread tokio) and failed for `server_uring.rs`.

It dates from when sorted-set and stream commands were first added, long
before the RESP3 work.

### It was not only an io_uring bug

`block_in_place` tells tokio to hand the worker's queue to a *replacement
thread* for the duration of the block. On the path where it "worked", a
thousand clients parked in `BLPOP key 0` cost a thousand OS threads. Removing
it makes a blocked client cost a task.

## Why it went unnoticed

`server_uring.rs` is `#[cfg(target_os = "linux")]`, so a Windows or macOS
development machine never compiles it, let alone runs it. Nothing in CI drove
real traffic through the io_uring path until a Linux verification workflow was
added; its very first run surfaced this.

## The fix

The wait moved out of the handler and into the connection loop, which is
already `async` — the same place `WAIT`/`WAITAOF` have always done their
waiting. A blocking command is now split across two dispatches:

1. **Park.** The handler runs its fast path, registers its waiters on the
   `WaitTable`, re-checks to close the TOCTOU window, and hands the registered
   receivers to `blocking::park_with_state` instead of blocking.
2. **Await.** The loop flushes any pipelined replies it is holding, then
   `.await`s. A plain `.await` behaves identically on both runtimes.
3. **Resume.** The loop re-dispatches the same command — it simply does not
   advance `parse_offset`, so the parse loop re-reads the same bytes — with the
   delivery in `Ctx::blocking`. The handler skips its fast path and runs only
   its tail.

All ten `block_in_place` call sites are gone; `grep block_in_place src/` returns
only documentation.

### Two things that are easy to get wrong here

**The resume must precede the fast path.** The delivered element was handed off
out-of-band by `LPUSH`/`ZADD` and is *not* in the keyspace. A handler that ran
its fast path first on resume could return a different element and silently drop
the delivered one.

**`XREAD BLOCK $` must not re-resolve `$`.** `$` means "entries added after this
command arrived". Re-resolving it on resume points past the very entry that woke
the client, so `XREAD BLOCK $` would return nil forever. The resolved IDs travel
with the park as `BlockState::StreamIds`.

### Cost to the hot path

None measurable. `SET` +2.6%, `GET` +4.4% against the build before the change
(median of 10 runs per arm) — i.e. within noise, in the favourable direction.

`Ctx` is constructed and moved on every command, so the blocking state is
carried as `Option<&mut BlockingCtl>` rather than by value: at the time,
`size_of::<Ctx>()` went from 280 bytes to 288, where by value it would have
been 440.

**A benchmarking warning, because this nearly produced a false finding.** The
first harness ran both servers concurrently and always measured baseline first
in each round. It reported `SET` as 8% slower, then 17% slower, "consistently,
in every round" — an artifact: the baseline server's post-write background work
overlapped the other arm's `SET` run. Two corrections were needed:

* one server alive at a time, and
* alternating order (ABBA), so neither arm is systematically measured second.

With those, the regression vanished. The size hypothesis was then tested
directly — the same binary with `Ctx` padded back to 440 bytes — and the padded
build was *faster* by 1.7%, i.e. `Ctx` size at this scale does not move
throughput at all. A consistent-looking direction across rounds is not evidence
when the rounds share a systematic bias.

### Behaviour change: blocking commands inside MULTI

Callers that cannot await — Lua, `MULTI`/`EXEC`, the replica applier — pass
`blocking: None`, and blocking commands return their empty-handed reply
immediately. That matches Redis, where a blocking command inside `MULTI` never
blocks. Lua already short-circuited these explicitly; `EXEC` did not.

### Also fixed in passing

`XREADGROUP BLOCK` framed its post-wake and race-recheck replies with
`resp_array` while the non-blocking return used `stream_results_reply`. Under
RESP3 that is the difference between a flat array and a map keyed by stream
name, so a RESP3 client got a structurally different reply whenever it happened
to block. Invisible under RESP2, and only reachable when blocking, which is why
the RESP3 sweep missed it.

## Verifying

`smoke/blocking_matrix.py` is the battery — 26 checks per protocol, run against
both I/O models in CI. It asserts not just that a reply
arrives but that it arrives *after* the pushing client acts (i.e. the command
really blocked), that the connection is still usable afterwards, that a parked
client does not stall other connections, and that pipelined replies are flushed
before parking.

io_uring cannot be exercised on Windows, and Docker's default seccomp profile
blocks the syscalls, so a container needs `--security-opt seccomp=unconfined`:

```bash
docker run --rm --security-opt seccomp=unconfined -v "$PWD:/w" python:3.12-slim \
  bash -c '/w/kividb --port 6390 --io-model uring & sleep 3; python /w/smoke/blocking_matrix.py --port 6390'
```

Without that flag the server starts, logs `I/O model: io_uring`, and then every
worker thread panics inside `tokio-uring` — which looks like a kividb bug and
is not one.

## The same trap, a second time: `CLIENT KILL`

`CLIENT KILL` did not close an idle client's socket under io_uring — the target
kept answering `PING`. Same root cause, different symptom: the tokio loop races
`kill_rx` against the socket read in a `select!`, which io_uring cannot do
(a losing read arm leaks its buffer, since the op cannot be cancelled). This
path therefore only checked `kill_rx` *before* each read, so the kill did not
take effect until the client happened to send something — and an idle client
never does.

The fix does not need cancellation either. A watcher task awaits `kill_rx` and
calls `shutdown(fd, SHUT_RDWR)`; that makes the in-flight read *complete* on its
own, and the loop exits by the ordinary `Ok(0)` close path. Redis sends no reply
to a killed client, so nothing else is needed.

## The same trap, a third time: RESP2 subscriber mode dropped commands

Subscriber mode needs to wait on two things — broadcast deliveries and client
commands — and did it by wrapping the socket read in
`tokio::time::timeout(1ms, …)`, looping on the elapsed case. The old comment
blamed buffer lifetimes for avoiding `select!`, but a timeout has the *same*
hazard: dropping the future does not cancel the submitted op, so the kernel
completes the read into a buffer nobody is listening to and those bytes leave
the stream.

The effect was not marginal. A subscriber issuing `PING` in a loop:

| I/O model | Unanswered |
|---|---|
| tokio | 0 / 60 |
| io_uring (before) | **56 / 60** |
| io_uring (after) | 0 / 60 |

So a RESP2 pub/sub client under `--io-model uring` could barely issue a command
at all — `PING` health checks, `SUBSCRIBE` to further channels, `UNSUBSCRIBE`,
all mostly discarded. `test_punsubscribe_stops_pattern_messages` failed 6/10 on
uring and 0/10 on tokio; it had been written off more than once as a flake.

The fix was the same shape as the other two: the read was never again
cancelled. Since then subscriber mode has been reworked: a subscribed
connection stays in its ordinary command loop, whose reads are never
cancelled, and a separate task writes its messages to the socket under the
connection's shared write guard.

Worth noting as a pattern: three times now, the io_uring answer to "tokio races
X against the read" has been "make the read finish" rather than "cancel the
read". If you find yourself reaching for `select!` or `timeout` around a
`tokio_uring` op, that is the bug, not the solution.

(`test_unsubscribe_one_keep_others`, previously listed here as failing, no
longer reproduces; those pub/sub failures were timing-dependent and the specific
test varied between runs.)

## Found at the same time

`MULTI`/`EXEC` was not isolated from concurrent reads, on both I/O models and
unrelated to blocking commands. It is fixed; see
[EXEC_ATOMICITY.md](EXEC_ATOMICITY.md).
