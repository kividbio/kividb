# Redis 8.8 command support (KiviDB v1.0.4)

KiviDB v1.0.4 implements the command set introduced in Redis 8.8: the 18-command
**array** type, plus `INCREX` and `XNACK`.

| Group  | Commands | Status |
|--------|----------|--------|
| Array  | `ARSET` `ARMSET` `ARGET` `ARMGET` `ARGETRANGE` `ARDEL` `ARDELRANGE` `ARLEN` `ARCOUNT` `ARINSERT` `ARRING` `ARNEXT` `ARSEEK` `ARSCAN` `ARGREP` `AROP` `ARLASTITEMS` `ARINFO` | Implemented |
| String | `INCREX` | Implemented |
| Stream | `XNACK` | Implemented |
| Stream | `XIDMPRECORD` | **Not implemented** — see below |

`XIDMPRECORD` is documented by Redis as an internal command for setting IDMP
metadata on an existing stream message. It has no client-facing contract to
implement against and no observable behaviour a client can depend on, so it is
deliberately omitted rather than guessed at.

---

## The array type

An array is a **sparse**, index-addressed collection of string values with an
**insert cursor**. `ARSET k 1000000 v` stores one element, not a million.

- `ARCOUNT` — number of non-empty elements.
- `ARLEN` — highest occupied index **+ 1** (so a lone element at index 5 gives
  `ARLEN` 6 and `ARCOUNT` 1).
- `ARNEXT` — where the next `ARINSERT` will write.
- `TYPE` reports `array`.

### Storage model

Redis backs this type with a directory of fixed-width slices. KiviDB uses a
`BTreeMap<u64, Arc<[u8]>>` (`ArrayInner`, `src/types.rs`).

This is a deliberate trade. Ordered iteration gives the range commands
(`ARGETRANGE`, `ARSCAN`, `ARGREP`, `AROP`, `ARDELRANGE`) exactly the complexity
their command pages document — proportional to the number of *existing* elements
touched, not to the numeric span of the requested range — without needing a slice
walk. The cost is per-element overhead on dense arrays, the same trade this
codebase already makes for sorted sets.

The consequence you can observe: `ARINFO`'s slice-shaped fields (`slices`,
`directory-size`, `super-dir-entries`, and the `FULL` statistics) are **derived**,
not literal. They are computed by bucketing occupied indices into 4096-wide
windows — matching Redis's own default `slice-size` — so the reply carries the
same field names with values that mean the same thing. `dense-slices` counts
buckets at ≥50% occupancy.

### Ring buffers

`ARRING key size value [value ...]` writes at `next_insert % size`, wrapping so
newer values overwrite older ones within a fixed window. Shrinking `size`
truncates the array to the new window. The ring size is part of the value and
survives persistence.

`ARLASTITEMS` walks backwards from the cursor, wrapping inside the ring, which is
what produces the ordering in Redis's own `ARRING` example (`v1, v2, v3` after
inserting `v0..v3` into a size-3 ring).

---

## Spec ambiguities, and how they were resolved

Two array command pages contradict themselves. Both are resolved here in favour
of the normative **"Return information"** section over the rendered `redis>`
example, since the former is the specification and the latter is illustrative.

### 1. `ARSCAN` and `ARGREP WITHVALUES` reply shape

Both pages' "Return information" sections stated the reply is a **flat** array:
`[idx1, val1, idx2, val2, ...]`. Both pages' examples render *nested* pairs:

```
redis> ARSCAN myarray 0 10
1) 1) (integer) 0
   2) "a"
```

**KiviDB returns nested pairs, as Redis 8.10 does** (its test suite checks
`{0 a} {5 b} {9 c}`). Before 1.1.1 KiviDB returned the flat form.

### 2. `ARGETRANGE` on an unbounded span

`ARGETRANGE` is the only range command specified to materialise empty slots as
nils, which makes its reply size proportional to `|end - start|` rather than to
the number of elements stored. On a sparse array that span is attacker-controlled
and effectively unbounded — `ARGETRANGE key 0 9223372036854775806` would need an
exabyte-scale reply.

Redis does not document a limit. **KiviDB rejects spans wider than 1,000,000
indices** with an error pointing at `ARSCAN`, which visits only existing elements
and is safe over any span:

```
redis> ARGETRANGE k 0 9223372036854775806
(error) ERR range spans ... which exceeds the 1000000 limit for ARGETRANGE;
        use ARSCAN to iterate existing elements instead

redis> ARSCAN k 0 9223372036854775806     # fine, any span
1) (integer) 0
2) "v"
```

### 3. Negative indices

Array indices are documented as zero-based; the pages do not say what a negative
index means. KiviDB **rejects** them rather than treating them as end-relative
the way list and string commands do — an array's `len` is "max index + 1" over a
sparse space, so there is no element for `-1` to name.

---

## Other implementation notes

### `ARGREP RE`

The `RE` predicate is backed by the `regex` crate — a linear-time engine with no
backtracking, so a client-supplied pattern cannot be turned into a CPU
denial-of-service the way a backtracking engine allows. `NOCASE` is applied by
compiling the pattern with the engine's own `(?i)` flag rather than by
lowercasing the pattern text, which would corrupt character classes.

`GLOB` uses the same matcher as `KEYS` and `SCAN MATCH`.

### Atomicity

`ARMSET` and `ARDELRANGE` parse and validate every pair before touching the
store, so a bad index in the last pair does not leave the earlier ones applied.
`ARINSERT` refuses a whole batch that would run past the addressable end rather
than writing a partial prefix and leaving the cursor somewhere the reply does not
describe.

### Key lifecycle

`ARSEEK` deliberately does **not** create a key — its `1`/`0` reply is how a
caller distinguishes "cursor moved" from "no such key", which only means
something if a miss stays a miss.

A key is removed when it has no elements *and* no cursor or ring state. An array
that has only had elements deleted still holds a meaningful `next_insert`, so it
is kept.

### Replication, persistence, ACL

- **Replication:** `ARSET`, `ARMSET`, `ARDEL`, `ARDELRANGE`, `ARINSERT`,
  `ARRING` and `ARSEEK` propagate. `ARSEEK` counts as a write because it moves
  the insert cursor — persisted state a replica must mirror or its
  `ARINSERT`/`ARNEXT` would diverge from the master's.
- **Snapshots:** type tag 11. Elements are written as `(index, value)` pairs, so
  a sparse array costs its element count on disk, not its `ARLEN`. The cursor and
  ring size are persisted alongside.
- **AOF rewrite:** emitted as `ARRING` (if a ring), then `ARMSET`, then `ARSEEK`
  to restore the cursor.
- **DUMP / RESTORE:** supported. Unlike `TimeSeries` and `VectorSet`, an array
  has no instance-local state, so it round-trips.
- **ACL:** new `@array` category. Members split into `@read`/`@write`
  automatically via `replication::is_write_command`, so `+@read`, `-@write` and
  the `readonly` keyword all cover array commands correctly.

---

## `INCREX`

Increment, bounds, and expiry in one atomic command. Replies with a two-element
array `[new_value, actual_increment]`.

`actual_increment` is `0` when a bound violation caused the operation to be
skipped — that is what lets a rate limiter reject a request without a second
read:

```
INCREX ratelimit:u1 BYINT 1 UBOUND 100 EX 60 ENX
```

Notes:

- A **skipped** operation leaves the key *and its TTL* untouched — the expiry
  options are not applied.
- Integer overflow with no explicit bound counts as a bound violation, per the
  spec's "a bound violation includes ... overflowing the type limits".
- `BYINT` (and the no-option default) requires the stored value to parse as an
  integer, consistent with `INCR`/`INCRBY`; a stored `"1.5"` is an error.
  `BYFLOAT` accepts either.
- `BYFLOAT` replies with bulk strings in both slots, matching how `INCRBYFLOAT`
  already behaves on this server (RESP3 doubles are not substituted).
- It is replicated, and written to the AOF, as where it left the key: `SET key
  <new value> KEEPTTL`, followed by `PEXPIREAT` or `PERSIST` if it changed the
  TTL — and as nothing at all when a bound made it skip. A replica therefore
  cannot round a float differently, re-evaluate `ENX` against a TTL that has
  moved, or restart a relative TTL.

## `XNACK`

Releases pending entries back to the group PEL without acknowledging them, so
another consumer can claim them immediately instead of waiting out a
`min-idle-time`. Released entries are marked unowned (empty consumer) and given a
delivery time of 0; `XPENDING` reports their idle time as `-1`.

- `SILENT` decrements the delivery counter, `FAIL` leaves it, `FATAL` pins it to
  `LLONG_MAX`. `RETRYCOUNT` overrides all three.
- `FORCE` creates unowned PEL entries for IDs that exist in the stream but are
  not pending. IDs absent from the stream are never invented.

**Known deviation:** Redis keeps NACKed entries in a dedicated zone at the head
of the PEL so they are *prioritised* over other pending entries during claims.
KiviDB's PEL is a `BTreeMap` keyed by stream ID, so ordering stays by ID. The
functional effect — immediate claimability regardless of `min-idle-time` — is
preserved via the delivery-time-0 marker; the relative *ordering* against other
already-idle entries is not.

---

## Verifying

```sh
cargo test --test array          # 29 tests, incl. every redis.io worked example
cargo test --test string-json    # includes 13 INCREX tests
cargo test --test streams        # includes 9 XNACK tests
```

The `spec_example_*` tests in `tests/array.rs` are transcriptions of the worked
examples on the redis.io command pages — same commands, same expected replies —
and are the closest thing to a conformance suite this type has.
