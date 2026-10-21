# Redis's test suite against KiviDB

Redis ships its own test suite (`tests/unit/**` in its source tree, driven by
`./runtest`). CI runs it against every build of KiviDB as the
`redis-compat` job. This document is about what that job measures, what a
pass over the whole suite found, and what still fails.

## The number was not measuring the suite

Until this change the job ran the suite as one `./runtest` with
`--timeout 30`, against one server, and reported the count of `[ok]` lines.

`--timeout` is not a limit on a test. It is how long the runner waits
without *any* test finishing before it gives up on the whole run. The
HyperLogLog fuzz tests take about 45 seconds of client-side Tcl each, so
the run ended there: **11 of the suite's units ran**, and the number
reported — about 445 — was whatever those eleven came to. Nothing after
`unit/hyperloglog` in the list was ever run, and no fix to anything after
it could move the number.

There was a second problem behind the first. With one server for the whole
run, a test that fails half-way leaves its state behind — a `maxmemory` that
was not reset, a client still blocked, a paused server — and the units after
it fail for that reason and not their own.

`scripts/redis_compat.sh` runs the suite **one unit at a time, each against
a server started empty**, with a no-progress limit of five minutes and a
limit of fifteen on a unit as a whole. A unit that hangs or dies costs
itself. The script prints a line per unit, writes the table to the job's
summary page, and keeps every unit's log as an artifact. It exits 0 whatever
the tests did: it is a measurement, not a gate.

```
scripts/redis_compat.sh <kividb-binary> <redis-checkout> [<output-dir>]
```

Tests that cannot run against a server the suite did not start itself are
skipped by tag, as before: `external:skip`, `needs:debug`, `needs:repl`,
`needs:save`, `needs:pfdebug`, `large-memory`, `cluster:skip`, `keysizes`.
Most of the suite's ninety files are replication, cluster and persistence
tests of that kind and contribute nothing either way.

## Where it stands

CI runs Redis 8.10.2's suite since 1.1.1, the release KiviDB reports itself
as. The figures below are from Redis 8.0.0's suite, which it ran before; the
CI job's summary has the current ones.

Redis 8.0.0's suite, on Linux, io_uring loop:

| | passed | failed | units ended early |
|---|---:|---:|---:|
| before, as CI ran it (one run, 11 units) | ~445 | — | — |
| before, one unit at a time | 648 | 103 | 18 |
| this change | 1,892 | 31 | 1 |

"Ended early" is a unit that stopped on an exception in the test code or a
test that never finished; the tests after that point in the unit did not run
and are counted nowhere.

Twenty-three of the thirty-six units that have tests an external server can
run now pass whole: `type/zset` (317 tests), `type/list` (251), `scripting`
(206), `type/set` (114), `type/string` (79), `type/stream` (73), `geo` (64),
`expire` (58), `tracking` (55), `bitops` (49), `keyspace` (45), `multi` (35),
`type/incr`, `scan`, `protocol`, `sort`, `bitfield`, `other`, `slowlog`,
`type/list-3`, `info-command`, `quit` and `type/list-2`.

## What the pass found

Most of the 1,200-odd tests that now pass passed because of differences in
replies — an error worded differently, an integer where Redis sends a
string. Those are listed in the changelog. Some were not cosmetic:

- **`COPY` aliased its source.** A copied list, set, hash, sorted set or
  stream shared its storage with the original; a write to either changed
  both.
- **A request could take the server down.** `*3000000000\r\n` made the
  parser reserve room for three billion arguments. And a request that was
  merely malformed — a length that is not a number — left the connection
  waiting for ever for the rest of something that was never going to be a
  request. Both are now `-ERR Protocol error: …` and a closed connection,
  with Redis's limits.
- **Client-side caching tracked the first key of a command only**, ignored
  `BCAST` and `REDIRECT`, and did not see writes made by scripts, expiry or
  eviction. A client relying on invalidations could serve stale data.
- **Geo positions were not stored in Redis's encoding.** Distances and
  search results differed from Redis's in the last digits, `GEOHASH`
  strings differed, and a dataset moved between the two would not have
  meant the same thing. See *Upgrading*, below.
- **`SCAN` cursors were offsets.** A key added or removed during a scan
  could make the scan skip or repeat others. Cursors are now positions in a
  stable order.
- **`UNLINK` was not dispatched** at all.
- **A client blocked on several keys could take another client's element.**
  Blocked on `k2 k3` and woken because `k3` was pushed to, it looked at
  `k2` first; if `k2` held an element pushed for a client that had been
  waiting there longer, it took that — and the other client stayed blocked
  on an empty list, with the element in `k3` unclaimed. One time in eight
  on the io_uring loop, where the two clients run on different threads.
- **A command that arrived as a script started could wait for ever.** It
  had passed the check that answers `BUSY`, and then waited for the script
  to end; a script that loops does not. If the command came from the client
  that was about to `SCRIPT KILL` it, nothing ever ended it.
- **`XREADGROUP` readers were not served in order**, and a blocked reader
  was not woken when its stream or group was deleted.
- **`EXPIRE … GT` set a TTL on a key that had none**; **`ZADD` compared
  scores with a tolerance**, so two scores that differ in the last bits were
  the same score; **`RANDOMKEY` was not random**.
- **`WATCH` missed** a watched key expiring, `FLUSHALL` and `SWAPDB`.
- **Connection buffers never shrank** after one large request or reply.
- **Loading a library compared its name with every library's**: the
  ten-thousandth `FUNCTION LOAD` took ten thousand times the work of the
  first. The suite loads fifty thousand; that test alone took three and a
  half minutes, and now takes twelve seconds.
- **An error reply could contain a line break** — a Lua error from `FUNCTION
  LOAD` with its traceback — which ended the reply early and left the client
  reading the rest as the reply to its next command. And a `FUNCTION LOAD …
  REPLACE` that failed half-way left the new library's functions in place of
  the old ones.

## Two connection loops

KiviDB has two connection loops: io_uring, the default on Linux, and tokio,
used everywhere else and wherever the kernel does not allow io_uring — which
includes a container started without `--privileged` or
`--security-opt seccomp=unconfined`. The server says which it is running
when it starts (`I/O model`), and falls back without complaint.

That matters for this suite. The two loops share the commands but not the
code around them, and on io_uring a connection stays on the thread that
accepted it, so two clients woken by one write really do run at once. The
last two bugs in the list above, and three smaller ones — `CLIENT KILL …
SKIPME no` closing the caller before its reply, `tracking_clients` not
going down when a tracking connection ended on an error, and
`rdb_changes_since_last_save` missing changes made on different threads —
showed on io_uring and not, or hardly ever, on tokio. Run the suite where
io_uring is allowed; CI's runners allow it.

The numbers in this document are from the io_uring loop.

One test in `type/list` — "Circular BRPOPLPUSH" — fails in about one run
in four on io_uring; it passed in the run the table is from. Two clients
each move an
element to the list the other is blocked on; Redis serves both inside the
push that started it, KiviDB wakes them to serve themselves, and a command
that arrives in between sees the element half-way round.

## Upgrading

Everything visible to a client is under *Changed* in the changelog. Three
need action or attention:

1. **Geo keys must be re-added.** Positions are stored as Redis stores them.
   A geo key written by v1.0.4 or earlier decodes to the wrong coordinates
   after the upgrade — there is no telling an old score from a new one, so
   there is no migration. Delete the key and `GEOADD` its members again.
2. **RESP2 clients that use `CLIENT TRACKING` without `REDIRECT`** received
   invalidations as pub/sub frames they had not subscribed to. They now
   receive nothing, as in Redis. Use `HELLO 3`, or redirect to a connection
   subscribed to `__redis__:invalidate`.
3. **`CLIENT PAUSE timeout ALL` pauses reads.** It paused writes only. An
   operator script that pauses, reads, and unpauses on one connection still
   works — `CLIENT UNPAUSE` is never held — but its reads in between are.

## What still fails

Thirty-one tests, in three groups.

**Things KiviDB does not have (17).**

| unit | tests | what is missing |
|---|---:|---|
| `info-keysizes` | 5 | the key-size histograms of `INFO keysizes` |
| `lazyfree`, `functions` | 8 | the `lazyfreed_objects` and `lazyfree_pending_objects` counters, `used_memory_vm_functions`, and `FLUSHALL ASYNC` as a blocking background job. KiviDB frees in the command; the tests are of the counters |
| `latency-monitor` | 2 | the `expire-cycle` latency event |
| `querybuf` | 2 | `CLIENT LIST` does not report `idle=` (`idle=0` always), so the unit's idle-wait never satisfies — and on io_uring the idle shrink also has no timer (see below). The second failure is a knock-on of the first, not a separate gap |

**Redis's internal encodings (5).** `dump` (2): `RESTORE … IDLETIME` and
`FREQ` are accepted and not applied. `type/hash` (2) and the one unit that
still ends early, `type/stream-cgroups`: the tests `RESTORE` a payload
produced by Redis itself; KiviDB's `DUMP` format is its own and it does not
read Redis's. `hyperloglog` (1): the test reads the cache-valid bit of
Redis's HyperLogLog header with `GETRANGE`.

**Behaviour that differs (9).**

- `pause` (2): keys go on expiring while clients are paused; Redis stops
  expiry for the length of the pause.
- `introspection` (2): `CONFIG SET maxmemory-clients` and `CONFIG SET port`
  are accepted and remembered without being applied, so the tests'
  out-of-range value and unusable port are not refused.
- `introspection-2` (1): `TTL`, `TYPE` and `EXISTS` reset a key's idle time.
- `pubsub` (2): a RESP3 client that publishes to a channel it is subscribed
  to, from inside a script or a transaction, gets the message before the
  reply to the script; Redis sends the reply first.
- `type/stream-cgroups` (2): `XINFO STREAM … FULL` lists the idempotence
  fields of Redis 8.6, which the 8.0 suite does not expect; and an
  `XREADGROUP` that only re-reads its own pending entries is counted in
  `rdb_changes_since_last_save`, where Redis does not count it.

Expiry during a pause and the idle-time reset are the two of these worth
fixing next: both are real differences an application could meet.

### The query-buffer unit

Two tests fail here, and they have different causes. Neither is the
query-buffer sizing this change adds: `qbuf` and `qbuf-free` are reported
truthfully, and a declared fat argument reserves correctly.

**`query buffer resized correctly`** writes a partial command, lets the client
go idle, and waits on `[client_idle_sec test_client] >= 3` — the run of an idle
client Redis measures with its client cron. KiviDB reports `idle=0` in
`CLIENT LIST` (it does not yet track per-client idle time), so that condition
never becomes true and the test cannot reach the shrink it is looking for. The
immediate, shared cause is therefore the missing `idle=` reporting; it fails
identically on **both** Tokio and io_uring. The io_uring implementation has a
further limitation on top of it: it also has no timer to shrink an idle
buffer, because it cannot rebuild a buffer a read owns and will not cancel the
read to force one. The `idle=` fix is out of scope here and belongs in a
separate PR; once `idle=` is corrected, this test is expected to pass on
**Tokio** (its read is cancellable, so a timer can shrink the buffer while the
read waits). That is an expectation from reading the test and the code, not a
result we have measured.

**`query buffer resized correctly with fat argv`** sends the fat-argument
request on `$rd`, a separate `redis_client` connection, and runs `CLIENT LIST`
on `r`. It looks up the buffer with `client_query_buffer test_client`, which
takes the first `test_client` line from `CLIENT LIST`. The preceding test is
what fails first, and it fails *before* its `$rd close`, so its `test_client`
connection is still connected when this test runs — and its entry comes first.
`client_query_buffer` therefore reads the stale connection's entry, whose
`qbuf`/`qbuf-free` never grew, and the fat-argv assertion fails. This is a
knock-on of the first test failing, not a same-connection protocol effect and
not evidence that fat-argument reservation is broken (an isolated probe, and
the in-tree Rust test, show the reservation working).

## The cost

Checking a command's arity before it runs, recording each connection's last
command for `CLIENT LIST`, and counting each call in a latency histogram are
work every command now does. Measured as instructions executed per command
on a pipelined connection (callgrind, release profile), against the commit
before this change:

| | before | after | |
|---|---:|---:|---:|
| `PING` | 2,000 | 2,190 | +9.5%  |
| `GET` | 2,850 | 3,059 | +7.3%  |
| `SET` | 3,494 | 3,736 | +6.9%  |

Without pipelining the difference is not measurable: a round trip is two
system calls and a hundred times this.

## Running it locally

On Linux, with the server and `runtest` on the same machine:

```
git clone --depth 1 --branch 8.10.2 https://github.com/redis/redis.git /tmp/redis-tests
cargo build --release --features lua,tls
scripts/redis_compat.sh target/release/kividb /tmp/redis-tests /tmp/redis-compat
UNITS="unit/type/stream unit/geo" scripts/redis_compat.sh …   # some units only
```

Not from Windows or macOS through a container's network: every round trip
then costs milliseconds, and the fuzz tests — hundreds of thousands of
commands — take minutes or time out.

Reading a failure: `Expected 'X' to equal or match 'Y'` has the pattern the
test wanted first and what the server said second. `Expected 'A' to be equal
to 'B'` prints its arguments in the order the test passed them, and tests
pass them both ways round. Fix the first failure in a unit first; the ones
after it are often its consequences.
