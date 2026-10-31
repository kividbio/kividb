# KiviDB — Supported Commands

KiviDB implements a Redis-compatible command surface over the RESP protocol.
Commands are grouped by type. All commands are case-insensitive.

`HELP` lists the subcommands of `CLIENT`, `COMMAND`, `CONFIG`, `FUNCTION`,
`MEMORY`, `MODULE`, `OBJECT`, `PUBSUB` and `SLOWLOG` (for example `CLIENT HELP`).

Connect using any Redis-compatible client on port `6380` (default):
```bash
redis-cli -p 6380
```

---

## String commands

| Command | Syntax | Description |
|---------|--------|-------------|
| `SET` | `SET key value [NX \| XX \| IFEQ ifeq-value \| IFNE ifne-value \| IFDEQ ifdeq-digest \| IFDNE ifdne-digest] [GET] [EX seconds \| PX milliseconds \| EXAT unix-time-seconds \| PXAT unix-time-milliseconds \| KEEPTTL]` | Set a key to a string value; NX=only if absent, XX=only if present, IFEQ/IFNE=conditional on value match, IFDEQ/IFDNE=conditional on digest match, GET=return old value, EX/PX/EXAT/PXAT=set expiry, KEEPTTL=preserve existing expiry |
| `GET` | `GET key` | Get the value of a key |
| `GETDEL` | `GETDEL key` | Return the value and delete the key atomically |
| `GETSET` | `GETSET key value` | Return the old value and set a new one atomically |
| `GETEX` | `GETEX key [EX s \| PX ms \| EXAT ts \| PXAT ts \| PERSIST]` | Return the value and optionally modify its expiry |
| `GETRANGE` | `GETRANGE key start stop` | Return a byte substring using inclusive indices; negative indices count from the end |
| `SUBSTR` | `SUBSTR key start stop` | Alias for `GETRANGE` |
| `SETRANGE` | `SETRANGE key offset value` | Overwrite bytes at offset; zero-pads if key is shorter than offset |
| `SETEX` | `SETEX key seconds value` | Set a key with a TTL in seconds |
| `PSETEX` | `PSETEX key milliseconds value` | Set a key with a TTL in milliseconds |
| `SETNX` | `SETNX key value` | Set only if the key does not exist; returns 1 if set, 0 if skipped |
| `APPEND` | `APPEND key value` | Append bytes to an existing value; creates the key if absent; returns new length |
| `STRLEN` | `STRLEN key` | Return the byte length of a string value; 0 for missing keys |
| `INCR` | `INCR key` | Increment an integer value by 1; starts from 0 if key is absent |
| `INCRBY` | `INCRBY key increment` | Increment by an integer delta; negative delta decrements |
| `INCRBYFLOAT` | `INCRBYFLOAT key increment` | Increment by a float; result stored as string with trailing zeros trimmed |
| `INCREX` | `INCREX key [BYFLOAT increment \| BYINT increment] [LBOUND lower] [UBOUND upper] [SATURATE] [EX seconds \| PX milliseconds \| EXAT unix-time-seconds \| PXAT unix-time-milliseconds \| PERSIST] [ENX]` | Atomic increment with bounds and expiry. Replies `[new_value, actual_increment]`; `actual_increment` is 0 when a bound violation skipped the operation. SATURATE caps at the bound instead of skipping; ENX sets the TTL only if the key has none |
| `DECR` | `DECR key` | Decrement an integer value by 1 |
| `DECRBY` | `DECRBY key decrement` | Decrement by an integer delta |
| `MSET` | `MSET key value [key value ...]` | Set multiple key-value pairs; always succeeds |
| `MSETNX` | `MSETNX key value [key value ...]` | Set multiple pairs only if none of the keys exist; returns 1 or 0 |
| `MGET` | `MGET key [key ...]` | Return values for multiple keys; nil for missing or wrong-type keys |
| `LCS` | `LCS key1 key2 [LEN] [IDX] [MINMATCHLEN n] [WITHMATCHLEN]` | Find the longest common subsequence; LEN=return length only, IDX=return match positions, MINMATCHLEN=filter short matches, WITHMATCHLEN=include match lengths in IDX output |
| `DIGEST` | `DIGEST key` | Return the digest that `IFDEQ` / `IFDNE` compare against: the XXH3 64-bit hash of the string value, as 16 hex digits (the same value Redis returns); nil for a missing key |
| `DELEX` | `DELEX key [IFEQ val \| IFNE val \| IFDEQ digest \| IFDNE digest]` | Delete a key, optionally only if its string value (or digest) matches; the comparison and the deletion are one atomic step. Returns 1 if deleted, 0 if the key is missing or the condition is not met. Without a condition it deletes a key of any type |
| `MSETEX` | `MSETEX numkeys key value [key value ...] [NX \| XX] [EX seconds \| PX milliseconds \| EXAT unix-time-seconds \| PXAT unix-time-milliseconds \| KEEPTTL]` | Atomically set multiple key-value pairs with a shared expiry; NX/XX guard whether keys must be absent or present |

---

## Generic commands

Generic commands operate on keys regardless of their type. All key existence
checks respect lazy expiry — an expired key is treated as absent.

### Existence and deletion

| Command | Syntax | Description |
|---------|--------|-------------|
| `DEL` | `DEL key [key ...]` | Delete one or more keys; returns count of keys actually removed; ignores missing keys |
| `EXISTS` | `EXISTS key [key ...]` | Count how many of the given keys exist; the same key repeated counts multiple times |
| `UNLINK` | `UNLINK key [key ...]` | Alias for `DEL` — accepted for Redis compatibility |
| `TYPE` | `TYPE key` | Return the data type of a key: `string`, `list`, `set`, `zset`, `hash`, `stream`, `ReJSON-RL`, or `none` if missing/expired |
| `TOUCH` | `TOUCH key [key ...]` | Returns the count of keys that exist; expired keys count as absent; does not modify LRU |

### Serialization

| Command | Syntax | Description |
|---------|--------|-------------|
| `DUMP` | `DUMP key` | Serialize the value at key to a binary string in KiviDB's snapshot format with a 10-byte CRC64 envelope; returns nil if key missing or expired; TimeSeries and VectorSet keys are not portable |
| `RESTORE` | `RESTORE key ttl serialized-value [REPLACE] [ABSTTL] [IDLETIME seconds] [FREQ frequency]` | Deserialize a value from a `DUMP` payload and store it at key; ttl is in milliseconds (0 = no expiry); `REPLACE` overwrites an existing key; `ABSTTL` treats ttl as a Unix timestamp in milliseconds; returns `BUSYKEY` error if key exists without `REPLACE` |

### Sorting

| Command | Syntax | Description |
|---------|--------|-------------|
| `SORT` | `SORT key [BY pattern] [LIMIT offset count] [GET pattern [...]] [ASC\|DESC] [ALPHA] [STORE destination]` | Sort the elements of a list, set, or sorted set; numeric sort by default; `ALPHA` for lexicographic; `LIMIT` for pagination; `STORE` writes result as a List and returns count; `BY pattern` sorts by external string key or hash field (`hash_*->field`); `GET pattern` returns external values instead of elements; `GET #` returns the element itself |
| `SORT_RO` | `SORT_RO key [BY pattern] [LIMIT offset count] [GET pattern [...]] [ASC\|DESC] [ALPHA]` | Read-only variant of `SORT`; identical except `STORE` is rejected |

### Expiry — set and query

| Command | Syntax | Description |
|---------|--------|-------------|
| `EXPIRE` | `EXPIRE key seconds [NX \| XX \| GT \| LT]` | Set a TTL in seconds; returns 1 if set, 0 if key missing or condition not met; `NX`=only if no TTL exists, `XX`=only if TTL exists, `GT`=only if new TTL is greater than current, `LT`=only if new TTL is less than current; a key with no expiry counts as never expiring, as in Redis: `GT` does not set it, `LT` does |
| `PEXPIRE` | `PEXPIRE key milliseconds [NX \| XX \| GT \| LT]` | Set a TTL in milliseconds; same NX/XX/GT/LT conditions as `EXPIRE` |
| `EXPIREAT` | `EXPIREAT key unix-time-seconds [NX \| XX \| GT \| LT]` | Set expiry as an absolute Unix timestamp in seconds; a past timestamp expires the key immediately; same NX/XX/GT/LT conditions |
| `PEXPIREAT` | `PEXPIREAT key unix-time-milliseconds [NX \| XX \| GT \| LT]` | Set expiry as an absolute Unix timestamp in milliseconds; same NX/XX/GT/LT conditions |
| `PERSIST` | `PERSIST key` | Remove the TTL from a key; returns 1 if a TTL was removed, 0 if key had no TTL or does not exist |
| `TTL` | `TTL key` | Remaining TTL in seconds; `-1` = no expiry, `-2` = key not found or expired |
| `PTTL` | `PTTL key` | Remaining TTL in milliseconds; `-1` = no expiry, `-2` = key not found or expired |
| `EXPIRETIME` | `EXPIRETIME key` | Absolute Unix expiry timestamp in seconds; `-1` = no expiry, `-2` = key not found or expired |
| `PEXPIRETIME` | `PEXPIRETIME key` | Absolute Unix expiry timestamp in milliseconds; `-1` = no expiry, `-2` = key not found or expired |

### Key iteration

| Command | Syntax | Description |
|---------|--------|-------------|
| `KEYS` | `KEYS pattern` | Return all keys matching a glob pattern; O(N) over the full keyspace — avoid in production hot paths |
| `SCAN` | `SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]` | Cursor-based incremental key iteration; cursor `0` starts and ends a full cycle; `MATCH` filters by glob pattern (Redis's matching rules); `COUNT` is a hint for page size (default 10); `TYPE` returns only keys of that type; returns `[next-cursor, [keys...]]`. A cursor is a position in a stable order of key hashes, so a key present for the whole scan is returned whatever is added or removed meanwhile. Cursors from 1.0.4 or earlier are not valid |

### Key introspection

| Command | Syntax | Description |
|---------|--------|-------------|
| `OBJECT ENCODING` | `OBJECT ENCODING key` | The encoding Redis would use for the value, from its size and the `*-max-listpack-*` / `set-max-intset-entries` settings: `int`, `embstr` or `raw` for strings; `listpack` or `quicklist` for lists; `listpack`, `intset` or `hashtable` for sets; `listpack` or `hashtable` for hashes; `listpack` or `skiplist` for sorted sets; `stream`. KiviDB stores each type one way internally; the name is for clients and tests that look at it |
| `OBJECT IDLETIME` | `OBJECT IDLETIME key` | Seconds since the key was last read or written, while an LRU or LRM `maxmemory-policy` is selected; an error under an LFU policy, as in Redis. `0` under the policies that do not go by use (`noeviction`, the random ones, `volatile-ttl`): keys are not watched then |
| `OBJECT FREQ` | `OBJECT FREQ key` | The key's logarithmic use count (Redis's LFU counter, with `lfu-log-factor` and `lfu-decay-time`) while an LFU `maxmemory-policy` is selected; an error otherwise, as in Redis |
| `OBJECT REFCOUNT` | `OBJECT REFCOUNT key` | Returns `1` — semantically correct since every value is behind a single `Arc`; returns nil for missing or expired keys |
| `OBJECT HELP` | `OBJECT HELP` | List the `OBJECT` subcommands |

### Key management

| Command | Syntax | Description |
|---------|--------|-------------|
| `RENAME` | `RENAME source destination` | Rename a key; overwrites destination if it exists; returns `ERR` if source does not exist; preserves TTL |
| `RENAMENX` | `RENAMENX source destination` | Rename only if destination does not exist; returns 1 if renamed, 0 if destination exists |
| `COPY` | `COPY source destination [DB destination-db] [REPLACE]` | Copy the value of source to destination without deleting source; `REPLACE` overwrites destination if it exists; `DB` copies to a different database; returns 1 if copied, 0 if skipped |
| `MOVE` | `MOVE key db` | Move a key to another database; returns 1 if moved, 0 if key missing or already exists in target db |
| `SWAPDB` | `SWAPDB db1 db2` | Atomically swap the contents of two databases; all keys and expiries are exchanged |
| `RANDOMKEY` | `RANDOMKEY` | Return a random key from the current database; nil if the database is empty |

---

## List commands

| Command | Syntax | Description |
|---------|--------|-------------|
| `LPUSH` | `LPUSH key value [value ...]` | Prepend one or more values; returns new length |
| `RPUSH` | `RPUSH key value [value ...]` | Append one or more values; returns new length |
| `LPUSHX` | `LPUSHX key value [value ...]` | Prepend only if key exists; returns new length or 0 |
| `RPUSHX` | `RPUSHX key value [value ...]` | Append only if key exists; returns new length or 0 |
| `LPOP` | `LPOP key [count]` | Remove and return the first element; with count returns an array of up to count elements |
| `RPOP` | `RPOP key [count]` | Remove and return the last element; with count returns an array of up to count elements |
| `LLEN` | `LLEN key` | Return the number of elements |
| `LRANGE` | `LRANGE key start stop` | Return a range of elements; negative indices count from the end |
| `LINDEX` | `LINDEX key index` | Return the element at an index |
| `LSET` | `LSET key index element` | Set the element at an index |
| `LINSERT` | `LINSERT key BEFORE\|AFTER pivot element` | Insert an element before or after a pivot value |
| `LPOS` | `LPOS key element [RANK rank] [COUNT num-matches] [MAXLEN len]` | Return the index of matching elements; RANK skips the first n-1 matches (negative=scan from tail), COUNT=number of matches to return (0=all), MAXLEN=limit scan length |
| `LREM` | `LREM key count element` | Remove occurrences of element; count > 0 from head, < 0 from tail, 0 = all |
| `LTRIM` | `LTRIM key start stop` | Trim a list in-place to the specified inclusive range |
| `LMOVE` | `LMOVE source destination LEFT\|RIGHT LEFT\|RIGHT` | Pop from one end of source and push to one end of destination |
| `RPOPLPUSH` | `RPOPLPUSH source destination` | Pop from tail of source, push to head of destination; deprecated alias for LMOVE |
| `LMPOP` | `LMPOP numkeys key [key ...] LEFT\|RIGHT [COUNT count]` | Pop one or more elements from the first non-empty list |
| `BLPOP` | `BLPOP key [key ...] timeout` | Blocking pop from head; unblocks when an element is available or timeout expires |
| `BRPOP` | `BRPOP key [key ...] timeout` | Blocking pop from tail; unblocks when an element is available or timeout expires |
| `BLMOVE` | `BLMOVE source destination LEFT\|RIGHT LEFT\|RIGHT timeout` | Blocking LMOVE; blocks until source has an element |
| `BLMPOP` | `BLMPOP timeout numkeys key [key ...] LEFT\|RIGHT [COUNT count]` | Blocking LMPOP; blocks until any key has an element |
| `BRPOPLPUSH` | `BRPOPLPUSH source destination timeout` | Blocking RPOPLPUSH; deprecated, use BLMOVE instead |
| `LMOVEM` | `LMOVEM source destination LEFT\|RIGHT LEFT\|RIGHT [<COUNT count \| EXACTLY count> <OBO \| BULK>]` | Move several elements between lists in one atomic step (Redis 8.10); one element if no count is given. `COUNT` moves up to that many; `EXACTLY` moves that many or nothing (nil). `OBO` moves them one by one, as repeated `LMOVE` would; `BULK` moves them as a block, keeping their order. Returns the moved elements |
| `BLMOVEM` | `BLMOVEM source destination LEFT\|RIGHT LEFT\|RIGHT timeout [<COUNT count \| EXACTLY count> <OBO \| BULK>]` | Blocking `LMOVEM`: waits until the source has an element — or, with `EXACTLY`, enough of them |

Notes:

- The command that removes the last element of a list removes the key, as in Redis. The same holds for sets, sorted sets, hashes and vector sets; an empty stream stays.
- Clients blocked on the same key are served in the order they blocked. A blocked client is woken when the key is written by any command — a push, but also `RENAME`, `COPY`, `RESTORE`, `SORT … STORE`, a script or a transaction — and then takes the element its own command asks for (the head for `BLPOP`, the tail for `BRPOP`, `COUNT` of them for `BLMPOP`).

---

## Array commands (AR*)

Sparse, index-addressed collections of string values with an insert cursor. Reported as type `array` by the `TYPE` command. Introduced in Redis 8.8.

`ARCOUNT` is the number of non-empty elements; `ARLEN` is the highest occupied index **+ 1**. A lone element at index 5 gives `ARLEN` 6 and `ARCOUNT` 1.

| Command | Syntax | Description |
|---------|--------|-------------|
| `ARSET` | `ARSET key index value [value ...]` | Set one or more contiguous values starting at `index`; returns the number of previously-empty slots filled |
| `ARMSET` | `ARMSET key index value [index value ...]` | Set multiple index-value pairs, non-contiguous and in any order; returns the number of previously-empty slots filled |
| `ARGET` | `ARGET key index` | Get the value at an index, or nil |
| `ARMGET` | `ARMGET key index [index ...]` | Get values at multiple indices; order is preserved and empty slots return nil |
| `ARGETRANGE` | `ARGETRANGE key start end` | Get every slot in the range including nils for empty ones; `start > end` returns the range in reverse. Spans wider than 1,000,000 indices are rejected — use `ARSCAN` |
| `ARDEL` | `ARDEL key index [index ...]` | Delete elements at the given indices; returns the number actually removed |
| `ARDELRANGE` | `ARDELRANGE key start end [start end ...]` | Delete elements in one or more inclusive ranges; overlapping ranges count each element once |
| `ARLEN` | `ARLEN key` | Highest occupied index + 1, or 0 |
| `ARCOUNT` | `ARCOUNT key` | Number of non-empty elements, or 0 |
| `ARINSERT` | `ARINSERT key value [value ...]` | Insert at the cursor, advancing it once per value; returns the last index written |
| `ARRING` | `ARRING key size value [value ...]` | Insert into a ring buffer of `size`, wrapping at `size` so newer values overwrite older ones; shrinking `size` truncates. Returns the last index written |
| `ARNEXT` | `ARNEXT key` | The index the next `ARINSERT` would use; 0 for a missing key, nil when the cursor is exhausted |
| `ARSEEK` | `ARSEEK key index` | Move the insert cursor; returns 1 if set, 0 if the key does not exist. Does **not** create the key |
| `ARSCAN` | `ARSCAN key start end [LIMIT limit]` | Iterate existing elements in a range, skipping empty slots. Returns an array of `[idx, val]` pairs, as Redis 8.8 does; `start > end` iterates in reverse |
| `ARGREP` | `ARGREP key start end <EXACT string \| MATCH string \| GLOB pattern \| RE pattern [...]> [AND \| OR \| LIMIT limit \| WITHVALUES \| NOCASE ...]` | Search elements by textual predicate. `start`/`end` accept `-` (first index) and `+` (last). Multiple predicates combine with OR by default. `WITHVALUES` returns an array of `[idx, val]` pairs, as Redis 8.8 does, and without it an array of indices |
| `AROP` | `AROP key start end <SUM \| MIN \| MAX \| AND \| OR \| XOR \| MATCH value \| USED>` | Aggregate over non-empty elements in a range. SUM/MIN/MAX return bulk strings and reply nil when the range holds no numeric elements; AND/OR/XOR/MATCH/USED return integers |
| `ARLASTITEMS` | `ARLASTITEMS key count [REV]` | The most recently inserted elements, oldest-first by default, newest-first with `REV`. Wraps within the window for ring buffers |
| `ARINFO` | `ARINFO key [FULL]` | Array metadata: `count`, `len`, `next-insert-index`, `slices`, `directory-size`, `super-dir-entries`, `slice-size`, as in Redis 8.8. `FULL` adds per-slice statistics. RESP2 returns a flat array, RESP3 a map |

Notes:

- Indices are zero-based and **non-negative** — negative indices are rejected, not treated as end-relative.
- Arrays are sparse: `ARSET k 1000000 v` stores one element, not a million.
- KiviDB stores arrays as a sparse map rather than Redis's slice directory, so `ARINFO`'s slice-shaped fields are derived from 4096-wide index buckets. See [`docs/REDIS_8_8_COMMANDS.md`](docs/REDIS_8_8_COMMANDS.md).

---

## Set commands

Set values are unordered collections of unique byte strings, reported as type `set` by the `TYPE` command.

| Command | Syntax | Description |
|---------|--------|-------------|
| `SADD` | `SADD key member [member ...]` | Add one or more members; returns count of new members added |
| `SCARD` | `SCARD key` | Return the number of members; 0 if key missing |
| `SISMEMBER` | `SISMEMBER key member` | Return 1 if member exists, 0 otherwise |
| `SMISMEMBER` | `SMISMEMBER key member [member ...]` | Return an array of 1/0 for each member |
| `SMEMBERS` | `SMEMBERS key` | Return all members; order is not guaranteed |
| `SREM` | `SREM key member [member ...]` | Remove one or more members; returns count actually removed |
| `SPOP` | `SPOP key [count]` | Remove and return one or more random members |
| `SRANDMEMBER` | `SRANDMEMBER key [count]` | Return random members without removing; negative count allows repeats |
| `SMOVE` | `SMOVE source destination member` | Move a member from source to destination atomically; returns 1 if moved, 0 if not found |
| `SDIFF` | `SDIFF key [key ...]` | Return members in the first set not present in any subsequent set. As for `SINTER` and `SUNION`, a key that does not exist is an empty set and a key of another type is a `WRONGTYPE` error |
| `SDIFFSTORE` | `SDIFFSTORE destination key [key ...]` | Store the diff result in destination, replacing whatever was there (and its TTL); returns count of members stored. An empty result leaves no destination key — likewise for `SINTERSTORE` and `SUNIONSTORE` |
| `SINTER` | `SINTER key [key ...]` | Return members present in all given sets |
| `SINTERSTORE` | `SINTERSTORE destination key [key ...]` | Store the intersection in destination; returns count of members stored |
| `SINTERCARD` | `SINTERCARD numkeys key [key ...] [LIMIT limit]` | Return the cardinality of the intersection; LIMIT caps the count |
| `SUNION` | `SUNION key [key ...]` | Return all members from all given sets combined |
| `SUNIONSTORE` | `SUNIONSTORE destination key [key ...]` | Store the union in destination; returns count of members stored |
| `SSCAN` | `SSCAN key cursor [MATCH pattern] [COUNT count]` | Incrementally iterate members; cursor 0 begins and ends a full iteration. Cursors are stable positions, as for `SCAN` |
| `SUNIONCARD` | `SUNIONCARD numkeys key [key ...] [APPROX] [LIMIT limit]` | Return the cardinality of the union without building it for the client (Redis 8.10); `LIMIT` stops counting once it is reached. `APPROX` is accepted; the count returned is exact |
| `SDIFFCARD` | `SDIFFCARD numkeys key [key ...] [LIMIT limit]` | Return the cardinality of the difference between the first set and the others (Redis 8.10) |

---

## Hash commands

| Command | Syntax | Description |
|---------|--------|-------------|
| `HSET` | `HSET key field value [field value ...]` | Set one or more fields; returns count of fields added |
| `HGET` | `HGET key field` | Return the value of a field; nil if field or key missing |
| `HMSET` | `HMSET key field value [field value ...]` | Deprecated alias for `HSET`; always returns OK |
| `HMGET` | `HMGET key field [field ...]` | Return values for multiple fields; nil for missing fields |
| `HGETALL` | `HGETALL key` | Return all fields and values as alternating field, value, field, value... |
| `HDEL` | `HDEL key field [field ...]` | Delete one or more fields; returns count removed |
| `HEXISTS` | `HEXISTS key field` | Returns 1 if field exists, 0 otherwise |
| `HLEN` | `HLEN key` | Return the number of fields |
| `HKEYS` | `HKEYS key` | Return all field names |
| `HVALS` | `HVALS key` | Return all field values |
| `HINCRBY` | `HINCRBY key field increment` | Increment an integer field; initialises to 0 if absent |
| `HINCRBYFLOAT` | `HINCRBYFLOAT key field increment` | Increment a float field; initialises to 0 if absent |
| `HSETNX` | `HSETNX key field value` | Set a field only if it does not already exist; returns 1 if set |
| `HRANDFIELD` | `HRANDFIELD key [count [WITHVALUES]]` | Return random field(s); negative count allows repeats |
| `HSCAN` | `HSCAN key cursor [MATCH pattern] [COUNT count] [NOVALUES]` | Incrementally iterate fields and values; `NOVALUES` returns field names only. Cursors are stable positions, as for `SCAN` |
| `HEXPIRE` | `HEXPIRE key seconds [NX \| XX \| GT \| LT] FIELDS numfields field [field ...]` | Set per-field TTLs in seconds; per-field returns: `-2`=no such field, `0`=condition not met, `1`=TTL set, `2`=field deleted (past time) |
| `HPEXPIRE` | `HPEXPIRE key milliseconds [NX \| XX \| GT \| LT] FIELDS numfields field [field ...]` | Same as `HEXPIRE` but TTL in milliseconds |
| `HEXPIREAT` | `HEXPIREAT key unix-time-seconds [NX \| XX \| GT \| LT] FIELDS numfields field [field ...]` | Set per-field expiry as an absolute Unix timestamp in seconds; a past timestamp deletes the field immediately and returns `2` |
| `HPEXPIREAT` | `HPEXPIREAT key unix-time-milliseconds [NX \| XX \| GT \| LT] FIELDS numfields field [field ...]` | Same as `HEXPIREAT` but the timestamp is in milliseconds |
| `HTTL` | `HTTL key FIELDS numfields field [field ...]` | Per-field remaining TTL in seconds; per-field returns: `-2`=no such field, `-1`=field has no TTL, otherwise remaining seconds |
| `HPTTL` | `HPTTL key FIELDS numfields field [field ...]` | Same as `HTTL` but in milliseconds |
| `HEXPIRETIME` | `HEXPIRETIME key FIELDS numfields field [field ...]` | Per-field absolute Unix expiry in seconds; per-field returns: `-2`=no such field, `-1`=field has no TTL, otherwise Unix seconds |
| `HPEXPIRETIME` | `HPEXPIRETIME key FIELDS numfields field [field ...]` | Same as `HEXPIRETIME` but the timestamp is in milliseconds |
| `HPERSIST` | `HPERSIST key FIELDS numfields field [field ...]` | Remove per-field TTLs; per-field returns: `-2`=no such field, `-1`=field has no TTL, `1`=TTL removed |
| `HGETDEL` | `HGETDEL key FIELDS numfields field [field ...]` | Atomically read and delete fields; returns array of values, nil for missing fields |
| `HGETEX` | `HGETEX key [EX seconds \| PX ms \| EXAT unix-secs \| PXAT unix-ms \| PERSIST] FIELDS numfields field [field ...]` | Atomically read fields and optionally manipulate per-field TTLs; returns array of values, nil for missing fields |
| `HSETEX` | `HSETEX key [FNX \| FXX] [EX seconds \| PX milliseconds \| EXAT unix-seconds \| PXAT unix-milliseconds \| KEEPTTL] FIELDS numfields field value [field value ...]` | Atomically set fields, with one TTL for all of them if one is given; `FNX`=only if none of the fields exist, `FXX`=only if all of them do, `KEEPTTL`=leave each field's TTL as it is; with no TTL option the fields end up without one. Returns 1 if applied, 0 if `FNX`/`FXX` blocked it. The older KiviDB form `HSETEX key seconds [FNX \| FXX] [KEEPTTL] FIELDS …` is still accepted |
| `HSTRLEN` | `HSTRLEN key field` | Length in bytes of the value at `field`; returns 0 if field or key is missing |
| `HIMPORT PREPARE` | `HIMPORT PREPARE fieldset field [field ...]` | Define a named list of field names for this connection (Redis 8.10). Fieldsets are private to the connection and go when it closes |
| `HIMPORT SET` | `HIMPORT SET key fieldset value [value ...]` | Create (or replace) the hash at `key` from a prepared fieldset and one value per field — bulk loading of many hashes with the same fields without repeating the names |
| `HIMPORT DISCARD` | `HIMPORT DISCARD fieldset` | Forget one fieldset; `HIMPORT DISCARDALL` forgets all of this connection's |

Notes:

- `HIMPORT` is the bulk-insertion command of Redis 8.10's compact hashes. KiviDB implements the command; the hash it creates is an ordinary hash (there is no separate compact encoding), and it is replicated and logged as `DEL` + `HSET`.
- Hash commands can also publish which **fields** they touched (Redis 8.8 subkey notifications), on four channel shapes, each switched on by its own `notify-keyspace-events` flag: `S` — `__subkeyspace@<db>__:<key>` carrying `<event>|<len>:<field>,…`; `T` — `__subkeyevent@<db>__:<event>` carrying `<len>:<key>|<len>:<field>,…`; `I` — `__subkeyspaceitem@<db>__:<key>\n<field>` carrying `<event>`, one message per field; `V` — `__subkeyspaceevent@<db>__:<event>|<key>` carrying the fields. Fields removed by the background expiry cycle are reported (`hexpired`); fields found expired by a read are not reported yet.

---

## JSON commands

JSON values are stored as parsed in-memory trees and reported as type `ReJSON-RL` by the `TYPE` command.

### Path syntax

Every command takes its path through one JSONPath engine. Two kinds of path
decide the shape of the reply:

| Form | Example | Behaviour |
|------|---------|-----------|
| JSONPath (`$…`) | `$.store.books[*].title` | Can match several values. The command answers for all of them: one entry per match, in document order, with nil where a match is not of the type the command works on. `JSON.GET` returns a JSON array of the matches (`[]` when there are none) |
| Legacy (`.a.b`, `a[0]`, `["a"]`) | `.store.name` | Answers for the first match alone, as a plain value; a match of the wrong type is an error |

A command given no path uses the legacy root (`.`).

| Syntax | Meaning |
|--------|---------|
| `$` | The document root |
| `.name` `['name']` `["name"]` | A member. Bracket notation takes any name; dot notation takes any characters except `. [ ] ( ) , = ! < > & \| * ~ ' " ? { }` and whitespace |
| `[0]` `[-1]` | An element; negative indexes count from the end |
| `.*` `[*]` | Every child of an object or array |
| `..name` `..*` `..[…]` | The same at any depth, in document order |
| `[a,b]` | Several selectors at once (a union) |
| `[start:end:step]` | An array slice; each part is optional, `step` may be negative |
| `[?expr]` `[?(expr)]` | The children — elements of an array, values of an object — for which `expr` holds; `@` is the child, `$` the root |
| `path~` | The member names of the objects matched, as strings (terminal) |

Inside a filter, and — from Redis 8.10 — as a whole query:

| | |
|---|---|
| Comparison | `==` `!=` `<` `<=` `>` `>=`, and `=~` (regular expression search; the pattern may be a literal or a path). `==` and `!=` compare any JSON value, including array and object literals, structurally; `1` equals `1.0` |
| Logic | `&&` `\|\|` `!` and parentheses |
| Arithmetic | `+` `-` `*` `/` `%`, unary `-` `+`. **Must be surrounded by spaces**: `@.a + 1` adds, `@.a+1` is a member named `a+1`. `/` always yields a float |
| Membership | `x in array`, `x nin array` |
| Set relations | `a subsetof b`, `a anyof b`, `a noneof b` |
| Size | `x sizeof n` (alias `size`), `x empty true\|false`, for strings, arrays and objects |
| Functions | `length()` `count()` `value()` `keys()` `match()` `search()` `concat()` `abs()` `ceiling()` `floor()` `first()` `last()` `index()` `min()` `max()` `avg()` `sum()` `stddev()` `append()` `subsetof()` `anyof()` `noneof()` — written `f(x, …)` or `x.f(…)` |

A query that is a plain path *selects* nodes. Anything else — `$.a + 1`,
`$.arr.length()`, `length($.arr)`, `$.obj~` — is a *projection*: it computes
values, which the reading commands return, and which the writing commands
refuse (there is nowhere to write).

Evaluation never fails: an operand that cannot be evaluated — a type that does
not fit, a division by zero, an index out of range, the wrong number of
arguments — is *Nothing*, which makes a filter condition false and a projection
empty (`[]`). A path that cannot be parsed is an error.

```
JSON.GET doc '$..mountain_bikes[?(@.price < 3000 && @.specs.weight < 10)].model'
JSON.GET doc '$.a[?@ in $.allow]'
JSON.GET doc '$.books[?(@.price >= 10)].append({"t":"X"})'
JSON.GET doc '($.a + $.b) / 2' '$.arr.length()'
JSON.SET doc '$.inventory.*[?(@.price<2000)].price' 1500
```

### Commands

Replies are described for a JSONPath; a legacy path gives the single value in
place of the array.

| Command | Syntax | Description |
|---------|--------|-------------|
| `JSON.SET` | `JSON.SET key path value [NX \| XX] [FPHA FP16 \| BF16 \| FP32 \| FP64]` | Set every value the path matches; if it matches nothing and ends in a member name under a single existing object, add that member. `NX` = only if the path matches nothing, `XX` = only if it matches. A new key must be created at the root. `FPHA` (Redis 8.8) sets the floating-point type arrays of floats are held in. Returns `OK`, or nil if nothing was set |
| `JSON.GET` | `JSON.GET key [INDENT indent] [NEWLINE newline] [SPACE space] [path ...]` | No path: the whole document. One path: a JSON array of its matches. Several: an object with one member per path. The three options format the output |
| `JSON.MGET` | `JSON.MGET key [key ...] path` | The path evaluated against each key, as JSON text; nil for a missing key or one of another type |
| `JSON.MSET` | `JSON.MSET key path value [key path value ...]` | `JSON.SET` for several key-path-value triples; every argument is checked before any key is touched |
| `JSON.DEL` | `JSON.DEL key [path]` | Delete every value the path matches; returns how many. The root deletes the key |
| `JSON.FORGET` | `JSON.FORGET key [path]` | Alias for `JSON.DEL` |
| `JSON.TYPE` | `JSON.TYPE key [path]` | The type of each match: `object`, `array`, `string`, `integer`, `number`, `boolean`, `null` |
| `JSON.STRLEN` | `JSON.STRLEN key [path]` | The length of each matched string; nil for other types |
| `JSON.STRAPPEND` | `JSON.STRAPPEND key [path] value` | Append a JSON string to each matched string; returns the new lengths |
| `JSON.TOGGLE` | `JSON.TOGGLE key path` | Flip each matched boolean; returns the new values as 1 and 0 (a legacy path: `"true"` or `"false"`) |
| `JSON.NUMINCRBY` | `JSON.NUMINCRBY key path number` | Add to each matched number. Returns the new values as one JSON array in a bulk string, `null` where the match is not a number — `"[1820,1972,null]"` (RESP3: an array of numbers). A legacy path returns the one new value. A result that is not a finite number fails the command and changes nothing |
| `JSON.NUMMULTBY` | `JSON.NUMMULTBY key path number` | The same, multiplying |
| `JSON.OBJKEYS` | `JSON.OBJKEYS key [path]` | The member names of each matched object, in the order they were added |
| `JSON.OBJLEN` | `JSON.OBJLEN key [path]` | The number of members of each matched object |
| `JSON.ARRLEN` | `JSON.ARRLEN key [path]` | The length of each matched array |
| `JSON.ARRAPPEND` | `JSON.ARRAPPEND key path value [value ...]` | Append to each matched array; returns the new lengths |
| `JSON.ARRPOP` | `JSON.ARRPOP key [path [index]]` | Remove and return an element (default -1, the last) from each matched array; an index out of range means the nearest end |
| `JSON.ARRINSERT` | `JSON.ARRINSERT key path index value [value ...]` | Insert before `index` in each matched array; returns the new lengths |
| `JSON.ARRTRIM` | `JSON.ARRTRIM key path start stop` | Keep the inclusive range in each matched array; returns the new lengths |
| `JSON.ARRINDEX` | `JSON.ARRINDEX key path value [start [stop]]` | The index of the first element equal to `value` in each matched array, or -1; `stop` is exclusive and 0 means the end |
| `JSON.CLEAR` | `JSON.CLEAR key [path]` | Empty each matched array and object and set each matched number to 0; returns how many values changed |
| `JSON.MERGE` | `JSON.MERGE key path value` | RFC 7396 merge patch at each match — members are merged, a null deletes, anything else replaces |
| `JSON.DEBUG MEMORY` | `JSON.DEBUG MEMORY key [path]` | The approximate size of each match in bytes |
| `JSON.RESP` | `JSON.RESP key [path]` | Each match in RESP: null as nil, booleans as `true`/`false`, integers as integers, other numbers and strings as bulk strings, an array as `[` followed by its elements, an object as `{` followed by alternating names and values |

Objects keep their members in the order they were written, as in Redis.

---

## Vector Search commands (FT.*)

KiviDB implements the RediSearch RESP command surface for vector similarity search over Hash and JSON keys.

### Index lifecycle

| Command | Syntax | Description |
|---------|--------|-------------|
| `FT.CREATE` | `FT.CREATE index ON HASH\|JSON PREFIX n prefix [...] [LANGUAGE lang] [LANGUAGE_FIELD field] [STOPWORDS n word …] SCHEMA field [AS name] type [options ...]` | Create a search index; supported field types: `TEXT`, `TAG`, `NUMERIC`, `VECTOR`; backfills existing keys on creation. TEXT words are indexed with their stems, as in Redis, in the index's `LANGUAGE` (English by default) or the one a document names in its `LANGUAGE_FIELD` (hashes); `NOSTEM` turns that off for a field, and words shorter than `MINSTEMLEN` (`search-min-stem-len`, 4) are not stemmed. Snowball stemmers for Arabic, Danish, Dutch, English, Finnish, French, German, Greek, Hungarian, Italian, Norwegian, Portuguese, Romanian, Russian, Spanish, Swedish, Tamil and Turkish; Redis's other languages are accepted and not stemmed. `ON HASH` indexes hashes (including ones with field TTLs) by field name. `ON JSON` indexes JSON documents by JSONPath (`$.title AS title TEXT`): a string is a TEXT or TAG value, an array of strings is several tags or words, a boolean is the tag `true` / `false`, a number a NUMERIC value, an array of numbers a vector. `FT.SEARCH` returns a JSON document whole under `$`, and `RETURN` takes schema names or paths |
| `FT.DROPINDEX` | `FT.DROPINDEX index` | Delete an index definition; does not delete the underlying data keys |
| `FT.ALTER` | `FT.ALTER index [SKIPINITIALSCAN] SCHEMA ADD field type [options ...]` | Add new fields to an existing index schema. TEXT/TAG/NUMERIC fields take effect immediately. VECTOR fields have a fresh live index provisioned and are backfilled from existing keys unless SKIPINITIALSCAN is given. Multiple VECTOR fields per index are supported; each is queryable by `@field_name` in KNN clauses |
| `FT.INFO` | `FT.INFO index` | Return index metadata: name, type, prefixes, field schemas, field count. For indexes with HNSW fields, additionally returns `hnsw_live_count` / `hnsw_tombstone_count` / `hnsw_tombstone_ratio` / `hnsw_compaction_in_progress` (mirroring the first HNSW field for backwards compatibility) plus an `hnsw_indexes` nested array with per-field stats. With a TEXT field, `num_terms` (words seen), `num_records` (word, field and document triples) and what the TEXT index holds in bytes: `text_terms_bytes`, `text_postings_bytes`, `text_docs_bytes` |
| `FT._LIST` | `FT._LIST` | Return a list of all index names |
| `FT.ALIASLIST` | `FT.ALIASLIST index` | The aliases that point to `index` (Redis 8.10) |
| `FT.ALIASADD` | `FT.ALIASADD alias index` | Create an alias pointing to an existing index; AOF-logged |
| `FT.ALIASDEL` | `FT.ALIASDEL alias` | Remove an alias; AOF-logged |
| `FT.ALIASUPDATE` | `FT.ALIASUPDATE alias index` | Update an alias; overwrites if it already exists; AOF-logged |

### Search

| Command | Syntax | Description |
|---------|--------|-------------|
| `FT.SEARCH` | `FT.SEARCH index query [VERBATIM] [LANGUAGE lang] [SCORER name] [PARAMS n k v ...] [RETURN n field ...] [LIMIT offset count] [TIMEOUT ms] [DIALECT 2]` | Search an index; supports pure KNN (`*=>[KNN k @field $param AS alias]`), pure text/filter queries (`@field:token`, `@field:{tag}`, `@field:[lo hi]`, `*`), and hybrid pre-filter KNN. A word also finds its stem (`VERBATIM`: not; `LANGUAGE`: stemmed in that language). Scores are Redis 8.10's: `SCORER` `BM25STD` (default), `BM25STD.TANH`, `BM25`, `TFIDF`, `TFIDF.DOCNORM`, `DISMAX`, `DOCSCORE`, `HAMMING` (no payloads: 0). `TIMEOUT` is milliseconds, `0` no limit; omitted, `search-timeout` (`FT.CONFIG TIMEOUT`) is used, which is `0` unless set. The scan, the order of the matches (and `SORTBY`), and a filtered KNN's nearest-neighbour step are held to it. A query cut short: `search-on-timeout` (`FT.CONFIG ON_TIMEOUT`) `return` replies with what was found, `total_results` being that partial count (RESP3 puts `Timeout limit was reached` in `warning`; over RESP2 the client gets no signal); `fail` and `return_strict` reply with that as an error. A query that finishes, however late, is not reported as timed out. In a cluster, each master reports whether its part timed out and the coordinator applies `ON_TIMEOUT` |
| `FT.AGGREGATE` | `FT.AGGREGATE index query [LOAD n @field … \| LOAD *] [ADDSCORES] [GROUPBY n @prop ... REDUCE fn n arg ... [AS name] …] [APPLY expr AS name] [FILTER expr] [SORTBY n @prop [ASC\|DESC] … [MAX n]] [LIMIT offset count] [TIMEOUT ms] [WITHCURSOR [COUNT n] [MAXIDLE ms]] [PARAMS …]` | Aggregate the documents a query matches. Reducers: `COUNT`, `COUNT_DISTINCT`, `COUNT_DISTINCTISH`, `SUM`, `AVG`, `MIN`, `MAX`, `STDDEV`, `QUANTILE`, `TOLIST` (a list of the distinct values), `FIRST_VALUE [BY @prop [ASC\|DESC]]`, `RANDOM_SAMPLE`, and `COLLECT` (Redis 8.8: the group's documents — `FIELDS * \| n @field …`, with `SORTBY`, `LIMIT` and `DISTINCT` inside the group). Groups come out in the order they first appear. `ADDSCORES` gives each row `@__score`. `WITHCURSOR` returns the first `COUNT` rows and a cursor for `FT.CURSOR READ`. On an `ON JSON` index, fields go by the names the schema gives their paths. `TIMEOUT` is the same as on `FT.SEARCH`, and holds the scan, a `KNN` step, and `GROUPBY`, `APPLY`, `FILTER` and `SORTBY` to it; a step cut short keeps what it finished, and `LIMIT` still applies. Each `FT.CURSOR READ` page of a timed-out aggregation carries the warning |
| `FT.HYBRID` | `FT.HYBRID index SEARCH query [YIELD_SCORE_AS name] VSIM @field $param [KNN count K k [EF_RUNTIME n]] [YIELD_SCORE_AS name] [COMBINE RRF\|LINEAR count [WINDOW w] [CONSTANT c] [ALPHA a] [BETA b] [YIELD_SCORE_AS name]] [LIMIT off cnt] [SORTBY count field [ASC\|DESC]] [NOSORT] [LOAD count @field ...] [LOAD *] PARAMS n k v ... [GROUPBY ...] [REDUCE ...] [APPLY expr AS name] [FILTER expr]` | Combined text + vector search with score fusion. Default fusion is RRF (rank-based). LINEAR uses min-max-normalized scores with ALPHA (text) and BETA (vector) weights. Returns ranked merged result list with hybrid_score, text_score, vsim_score per row (renameable via YIELD_SCORE_AS). Supports the full FT.AGGREGATE pipeline tail (GROUPBY/REDUCE/APPLY/FILTER) for further transformation. Does not take `TIMEOUT` (an error before the pipeline clauses, ignored among them), and `search-timeout` does not apply: it runs to the end |

### Hybrid search filter syntax

Pre-filters run before KNN (in `FT.SEARCH`) or before aggregation (in `FT.AGGREGATE`):

| Pattern | Example | Description |
|---------|---------|-------------|
| TAG match | `@category:{electronics}` | Exact match, case-insensitive |
| TAG union | `@category:{electronics\|furniture}` | Match any of the listed values |
| NUMERIC range | `@price:[0 200]` | Inclusive range; use `-inf` and `+inf` for open bounds |
| TEXT word | `@name:cable` | A whole word of the field, case-insensitive, as RediSearch matches it: `@name:cab` does not find `cable`; `cab*` (prefix), `*ble` (suffix), `*abl*` (infix) and `%cabel%` (fuzzy) do |
| AND | `(@category:{electronics} @price:[0 100])` | Both conditions must pass |
| OR | `(@category:{electronics} \| @category:{furniture})` | Either condition passes |
| Match all | `*` | No filter — equivalent to pure KNN |

Full hybrid query example:
```
(@category:{electronics} @price:[0 200])=>[KNN 10 @embedding $vec AS score]
```

### VECTOR field options

| Algorithm | Option | Description |
|-----------|--------|-------------|
| `FLAT` | `TYPE FLOAT32\|FLOAT64\|FLOAT16\|BFLOAT16 DIM n DISTANCE_METRIC L2\|COSINE\|IP` | Brute-force exact search; O(n) per query; best for < 50k vectors |
| `HNSW` | `TYPE FLOAT32\|FLOAT64\|FLOAT16\|BFLOAT16 DIM n DISTANCE_METRIC L2\|COSINE\|IP M n EF_CONSTRUCTION n` | Approximate graph-based search; O(log n) per query; best for > 50k vectors |

`TYPE` is the element type of the field's blobs: a hash holds `DIM` little-endian elements of it, and so does a query's vector parameter, as in Redis. A JSON document holds a number array whatever the type. The index keeps every vector as 32-bit floats, so a FLOAT16 or BFLOAT16 field takes as much memory as a FLOAT32 one. `INT8` and `UINT8` (Redis 8.2) are not supported.

HNSW indexes auto-compact when tombstone ratio exceeds 20% (and tombstone count exceeds 64). Compaction runs in a background thread using a shadow-index pattern — readers and writers see no pause. Manual triggers via `FT.DEBUG COMPACT index field` are also supported.

A schema may declare multiple VECTOR fields. KNN queries reference the field via `@field_name`: `*=>[KNN 10 @image_vec $q AS score]`. Each VECTOR field has its own live index, its own dimension, and its own distance metric.

### Tag and text utilities

| Command | Syntax | Description |
|---------|--------|-------------|
| `FT.TAGVALS` | `FT.TAGVALS index field` | Return all distinct values stored in a TAG field across indexed keys; sorted alphabetically; splits on `\|` and `,` |
| `FT.SPELLCHECK` | `FT.SPELLCHECK index query [DISTANCE n] [TERMS INCLUDE\|EXCLUDE dict ...]` | Return spelling suggestions for unrecognised query terms. Vocabulary is maintained incrementally on every indexed HSET, so query latency is independent of dataset size. Tokens are not removed from the vocabulary on document delete — full rebuild is via index drop and recreate |
| `FT.SYNUPDATE` | `FT.SYNUPDATE index group_id [SKIPINITIALSCAN] term [term ...]` | Create or update a synonym group; a query term matches every term of the groups it is in, in documents indexed before the group was made too (a quoted phrase is matched as written); AOF-logged |
| `FT.SYNDUMP` | `FT.SYNDUMP index` | Return all synonym groups as a flat term → [group_id] mapping |

### Dictionary commands

| Command | Syntax | Description |
|---------|--------|-------------|
| `FT.DICTADD` | `FT.DICTADD dict term [term ...]` | Add terms to a named global dictionary; returns count of newly added terms; AOF-logged |
| `FT.DICTDEL` | `FT.DICTDEL dict term [term ...]` | Remove terms from a dictionary; returns count removed; AOF-logged |
| `FT.DICTDUMP` | `FT.DICTDUMP dict` | Return all terms in a dictionary, sorted alphabetically |

### Debug and compatibility commands

| Command | Syntax | Description |
|---------|--------|-------------|
| `FT.EXPLAIN` | `FT.EXPLAIN index query [PARAMS …] [VERBATIM] [LANGUAGE lang] [DIALECT n]` | The query's plan as Redis writes one: `INTERSECT { … }`, `UNION { … }`, `NOT`, `OPTIONAL`, `TAG:@field { … }`, `NUMERIC {lo <= @field <= hi}`, `VECTOR { … }`, a word's stems as `+stem(expanded)`, one clause to a line. An error for a query that does not parse or an index that does not exist |
| `FT.EXPLAINCLI` | `FT.EXPLAINCLI index query` | The same plan as an array of its lines |
| `FT.PROFILE` | `FT.PROFILE index SEARCH\|AGGREGATE\|HYBRID [LIMITED] QUERY query …` | Runs the query and returns `[reply, profile]` (a map with `Results` and `Profile` under RESP3). The profile has the 8.x shape — `Shards` (total, parsing and GIL times, `Warning`, `Iterators profile`, `Result processors profile`) and `Coordinator`. The iterator tree is the query's; times are of the query as a whole, not per iterator |
| `FT.CONFIG GET` | `FT.CONFIG GET option\|pattern` | The query engine's settings by RediSearch's names (`TIMEOUT`, `DEFAULT_DIALECT`, `MINPREFIX`, `MAXSEARCHRESULTS`, `CURSOR_MAX_IDLE`, …), as `[name, value]` pairs; `*` for all |
| `FT.CONFIG SET` | `FT.CONFIG SET option value` | Set one; an unknown option or a value of the wrong kind is an error. Acted on: `CURSOR_MAX_IDLE` (the longest a cursor may sit unread), `MAXSEARCHRESULTS` (the largest `LIMIT` an `FT.SEARCH` may ask for), and `TIMEOUT` / `ON_TIMEOUT` (how long a query that names no `TIMEOUT` may run, `0`, the default, being no limit; and whether running out is an error or a partial reply). These two are also the server settings `search-timeout` and `search-on-timeout` (`CONFIG SET`, the config file): one value, whichever name sets it. The rest are kept and reported |
| `FT.CURSOR DEL` | `FT.CURSOR DEL index cursor_id` | Close a cursor. `OK`, or `Cursor does not exist` |
| `FT.CURSOR READ` | `FT.CURSOR READ index cursor_id [COUNT n]` | The next rows of an `FT.AGGREGATE … WITHCURSOR`, and the cursor to read on with — `0` when there are no more. A cursor holds the result as it was when the aggregation ran; it is dropped after `MAXIDLE` unread, and when its index is |
| `FT.DEBUG COMPACT` | `FT.DEBUG COMPACT index field` | Force-trigger HNSW graph compaction for the named field on the named index. Queues a background rebuild that drops tombstoned slots; returns OK immediately. Returns ERR if the index/field doesn't exist or the field isn't HNSW |

---

## Vector Set commands (V*)

Vector Sets are a standalone data structure (Redis 8+) — vectors stored directly at a key, independent of the FT.* index system. Think of them as sorted sets where the score dimension is a high-dimensional float vector.

### Core commands

| Command | Syntax | Description |
|---------|--------|-------------|
| `VADD` | `VADD key [REDUCE dim] [DISTANCE L2\|COSINE\|INNERPRODUCT] (FP32 \| VALUES num) vector element [SETATTR json] [NOQUANT\|Q8\|BIN] [EF n] [M n]` | Add or update an element with its vector; `FP32` accepts raw little-endian bytes, `VALUES n` accepts space-separated floats; returns 1 if new, 0 if updated; AOF-logged |
| `VREM` | `VREM key element [element ...]` | Remove one or more elements; returns count removed; AOF-logged |
| `VCARD` | `VCARD key` | Return the number of elements in the set; 0 if key does not exist |
| `VDIM` | `VDIM key` | Return the vector dimension; nil if key does not exist |
| `VISMEMBER` | `VISMEMBER key element` | Return 1 if element exists, 0 otherwise |

### Vector retrieval

| Command | Syntax | Description |
|---------|--------|-------------|
| `VEMB` | `VEMB key element [RAW]` | Return the vector as an array of float strings; with `RAW`, return raw little-endian FP32 bytes |
| `VGETATTR` | `VGETATTR key element` | Return the JSON attributes string attached to an element; nil if none |
| `VSETATTR` | `VSETATTR key element json` | Attach a JSON attributes string to an element; pass empty string `""` to remove; AOF-logged |

### Similarity search

| Command | Syntax | Description |
|---------|--------|-------------|
| `VSIM` | `VSIM key (ELE \| FP32 \| VALUES num) vector [WITHSCORES] [WITHATTRIBS] [COUNT num] [EF n] [FILTER expr] [FILTER-EF n] [NOTHREAD] [TRUTH]` | Return elements ordered by cosine similarity to the query vector; `FILTER` keeps the elements whose JSON attributes pass Redis's expression (`.color == "red" and .size > 3`, `"a" in .tags`, `.year in [2024, 2025]`, arithmetic, `not`, `**`); an element without a field it asks for does not pass. The search is exact, so `EF` and `FILTER-EF` change nothing; `ELE` queries by an existing element's vector; `WITHSCORES` gives each a similarity from the set's metric: for cosine, the default and Redis's only one, `(1 + cosine) / 2` as in Redis (1 the same direction, 0.5 at right angles, 0 opposite); for `IP` the dot product; for `L2` `1 / (1 + distance)` |

### Inspection and traversal

| Command | Syntax | Description |
|---------|--------|-------------|
| `VINFO` | `VINFO key` | Return metadata: `vector-dim`, `vector-count`, `quant-type` (always `f32`: vectors are kept at full precision, whatever `VADD` asked for), `index-type`, `max-level` |
| `VLINKS` | `VLINKS key element [WITHSCORES]` | Return nearest neighbours of an element (up to 16) as a single-layer array; approximated via brute-force cosine similarity |
| `VRANDMEMBER` | `VRANDMEMBER key [count]` | Return random elements; positive count returns distinct elements up to set size; negative count returns `\|count\|` elements with repetition allowed |
| `VRANGE` | `VRANGE key start end [count]` | Return elements in lexicographical range of element names; bounds follow ZRANGEBYLEX syntax: `[inclusive`, `(exclusive`, `-` for start, `+` for end |

### Dictionary commands (stub)

`FT.DICTADD`, `FT.DICTDEL`, and `FT.DICTDUMP` are accepted for client compatibility but have no effect — KiviDB does not implement full-text tokenisation or stopword dictionaries.

---

## Sorted set commands

Sorted set commands operate on sets of unique strings ordered by a floating-point score. Reported as type `zset` by the `TYPE` command.

| Command | Syntax | Description |
|---------|--------|-------------|
| `ZADD` | `ZADD key [NX\|XX] [GT\|LT] [CH] [INCR] score member [score member ...]` | Add or update members with scores; NX=only add new, XX=only update existing, GT/LT=conditional update, CH=return changed count, INCR=treat as increment |
| `ZCARD` | `ZCARD key` | Return the number of members |
| `ZSCORE` | `ZSCORE key member` | Return the score of a member, or nil |
| `ZMSCORE` | `ZMSCORE key member [member ...]` | Return scores for multiple members |
| `ZRANK` | `ZRANK key member [WITHSCORE]` | Return 0-based rank in ascending score order |
| `ZREVRANK` | `ZREVRANK key member [WITHSCORE]` | Return 0-based rank in descending score order |
| `ZINCRBY` | `ZINCRBY key increment member` | Increment a member's score; returns new score |
| `ZREM` | `ZREM key member [member ...]` | Remove members; returns count removed |
| `ZCOUNT` | `ZCOUNT key min max` | Count members with scores in range; supports `(` for exclusive bounds and `-inf`/`+inf` |
| `ZLEXCOUNT` | `ZLEXCOUNT key min max` | Count members in lexicographical range; bounds use `[` (inclusive) or `(` (exclusive); `-` and `+` for open ends |
| `ZRANGE` | `ZRANGE key start stop [BYSCORE\|BYLEX] [REV] [LIMIT offset count] [WITHSCORES]` | Return members in range; unified command supporting index, score, and lex modes |
| `ZRANGEBYSCORE` | `ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]` | Return members by score range (legacy alias) |
| `ZREVRANGEBYSCORE` | `ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]` | Return members by score range descending (legacy alias) |
| `ZRANGEBYLEX` | `ZRANGEBYLEX key min max [LIMIT offset count]` | Return members in lexicographical range |
| `ZREVRANGEBYLEX` | `ZREVRANGEBYLEX key max min [LIMIT offset count]` | Return members in reverse lexicographical range |
| `ZREVRANGE` | `ZREVRANGE key start stop [WITHSCORES]` | Return members by index descending (legacy alias) |
| `ZRANGESTORE` | `ZRANGESTORE dst src min max [BYSCORE\|BYLEX] [REV] [LIMIT offset count]` | Store result of ZRANGE into a key; returns count stored |
| `ZPOPMIN` | `ZPOPMIN key [count]` | Remove and return lowest-scoring members |
| `ZPOPMAX` | `ZPOPMAX key [count]` | Remove and return highest-scoring members |
| `BZPOPMIN` | `BZPOPMIN key [key ...] timeout` | Blocking ZPOPMIN; blocks until a member is available |
| `BZPOPMAX` | `BZPOPMAX key [key ...] timeout` | Blocking ZPOPMAX; blocks until a member is available |
| `ZMPOP` | `ZMPOP numkeys key [key ...] MIN\|MAX [COUNT count]` | Pop from first non-empty sorted set |
| `BZMPOP` | `BZMPOP timeout numkeys key [key ...] MIN\|MAX [COUNT count]` | Blocking ZMPOP |
| `ZRANDMEMBER` | `ZRANDMEMBER key [count [WITHSCORES]]` | Return random members; positive count = distinct, negative = with repetition |
| `ZSCAN` | `ZSCAN key cursor [MATCH pattern] [COUNT count] [NOSCORES]` | Cursor-based iteration over members and scores; `NOSCORES` returns members only. Cursors are stable positions, as for `SCAN` |
| `ZDIFF` | `ZDIFF numkeys key [key ...] [WITHSCORES]` | Return members in first set not in subsequent sets |
| `ZDIFFSTORE` | `ZDIFFSTORE destination numkeys key [key ...]` | Store ZDIFF result |
| `ZINTER` | `ZINTER numkeys key [key ...] [WEIGHTS ...] [AGGREGATE SUM\|MIN\|MAX\|COUNT] [WITHSCORES]` | Return intersection of sorted sets; `AGGREGATE COUNT` (Redis 8.8) scores each member by the number of input sets it is in |
| `ZINTERSTORE` | `ZINTERSTORE destination numkeys key [key ...] [WEIGHTS ...] [AGGREGATE SUM\|MIN\|MAX\|COUNT]` | Store ZINTER result |
| `ZINTERCARD` | `ZINTERCARD numkeys key [key ...] [LIMIT limit]` | Return cardinality of intersection |
| `ZUNION` | `ZUNION numkeys key [key ...] [WEIGHTS ...] [AGGREGATE SUM\|MIN\|MAX\|COUNT] [WITHSCORES]` | Return union of sorted sets; `AGGREGATE COUNT` (Redis 8.8) scores each member by the number of input sets it is in |
| `ZUNIONSTORE` | `ZUNIONSTORE destination numkeys key [key ...] [WEIGHTS ...] [AGGREGATE SUM\|MIN\|MAX\|COUNT]` | Store ZUNION result |
| `ZREMRANGEBYSCORE` | `ZREMRANGEBYSCORE key min max` | Remove members with scores in range |
| `ZREMRANGEBYRANK` | `ZREMRANGEBYRANK key start stop` | Remove members by index range |
| `ZREMRANGEBYLEX` | `ZREMRANGEBYLEX key min max` | Remove members in lexicographical range |

---

## Bitmap commands

Bitmap commands operate on strings as arrays of bits. Bitmaps are not a separate type — they are stored as `string` values and reported as type `string` by the `TYPE` command. Bit ordering within each byte is big-endian (MSB first): bit offset 0 is the most significant bit of byte 0.

| Command | Syntax | Description |
|---------|--------|-------------|
| `GETBIT` | `GETBIT key offset` | Return the bit value (0 or 1) at `offset`; returns 0 if key missing or offset beyond string length |
| `SETBIT` | `SETBIT key offset value` | Set or clear the bit at `offset`; auto-extends the string with zero bytes if needed; returns the old bit value; AOF-logged |
| `BITCOUNT` | `BITCOUNT key [start end [BYTE \| BIT]]` | Count the number of set bits; without range counts the whole string; `BYTE` mode (default) treats start/end as byte indices; `BIT` mode treats them as bit indices; negative indices count from the end |
| `BITPOS` | `BITPOS key bit [start [end [BYTE \| BIT]]]` | Find the position of the first set (1) or clear (0) bit; returns -1 if not found; for clear-bit search with no explicit end, returns the first bit past the string |
| `BITOP` | `BITOP <AND \| OR \| XOR \| NOT> destkey key [key ...]` | Perform a bitwise operation across one or more source keys and store the result at `destkey`; shorter strings are zero-padded; `NOT` requires exactly one source key; returns the length of the destination string in bytes; AOF-logged |
| `BITFIELD` | `BITFIELD key [GET encoding offset] [OVERFLOW <WRAP \| SAT \| FAIL>] [SET encoding offset value] [INCRBY encoding offset increment] [...]` | Perform arbitrary bitfield integer operations in a single call; encoding is `u<n>` for unsigned or `i<n>` for signed where n is the bit width (1–63 for unsigned, 1–64 for signed); `SET` returns the old value; `INCRBY` returns the new value; `OVERFLOW` controls wrap-around behaviour for the subsequent operations; multiple subcommands return an array of results; AOF-logged |
| `BITFIELD_RO` | `BITFIELD_RO key [GET encoding offset [GET encoding offset ...]]` | Read-only variant of `BITFIELD`; only `GET` subcommands are accepted |

---

## HyperLogLog commands

HyperLogLog commands provide probabilistic cardinality estimation using ~12KB of memory per key regardless of the number of unique elements observed. The standard error is 0.81%. HyperLogLog values are stored as `string` values using a Redis-compatible dense encoding (16384 registers, 6 bits each) and are reported as type `string` by the `TYPE` command.

| Command | Syntax | Description |
|---------|--------|-------------|
| `PFADD` | `PFADD key [element [element ...]]` | Add one or more elements to the HyperLogLog; creates the key if it does not exist; returns 1 if the internal representation was modified (cardinality estimate changed), 0 otherwise; calling with no elements creates the key (returns 1) or is a no-op if it exists (returns 0); AOF-logged |
| `PFCOUNT` | `PFCOUNT key [key ...]` | Return the approximated cardinality of the set observed by the HyperLogLog at `key`; with multiple keys, returns the cardinality of the union by merging into a temporary HyperLogLog without persisting it |
| `PFMERGE` | `PFMERGE destkey [sourcekey [sourcekey ...]]` | Merge one or more source HyperLogLogs into `destkey` by taking the element-wise maximum of all registers; missing source keys are treated as empty; AOF-logged |

### Debug and testing commands

| Command | Syntax | Description |
|---------|--------|-------------|
| `PFDEBUG GETREG` | `PFDEBUG GETREG key` | Return an array of all 16384 register values (0–63) for the HyperLogLog at `key`; useful for inspecting which registers have been set and to what depth |
| `PFDEBUG DECODE` | `PFDEBUG DECODE key` | Return a human-readable summary of the HyperLogLog internal state including encoding, register count, number of non-zero registers, maximum register value, and current cardinality estimate |
| `PFDEBUG ENCODING` | `PFDEBUG ENCODING key` | Return the encoding type; always `dense` as KiviDB uses only the dense representation |
| `PFDEBUG TODENSE` | `PFDEBUG TODENSE key` | No-op — KiviDB uses dense encoding exclusively; returns OK for compatibility |
| `PFDEBUG RESET` | `PFDEBUG RESET key` | Zero all registers and invalidate the cached cardinality, effectively resetting the HyperLogLog to empty while keeping the key; AOF-logged |
| `PFSELFTEST` | `PFSELFTEST` | Run internal correctness tests of the HyperLogLog implementation: verifies empty cardinality returns 0, estimation accuracy is within 2% for 10,000 elements, and register encode/decode round-trips correctly; returns OK on success or an error describing the failure |

---

## Geospatial commands

Geospatial commands operate on geographic coordinates. Geo indexes are stored as sorted sets where the score is a 52-bit geohash (interleaved longitude and latitude bits encoded as a float); they are reported as type `zset` by the `TYPE` command. Longitude must be in the range −180 to 180; latitude is restricted to approximately −85.05 to 85.05 (the Web Mercator projection limit). Distances are computed using the Haversine formula against an Earth radius of 6372797.56m.

### Core commands

| Command | Syntax | Description |
|---------|--------|-------------|
| `GEOADD` | `GEOADD key [NX \| XX] [CH] longitude latitude member [longitude latitude member ...]` | Add one or more members to a geospatial index; `NX` only adds new members (never updates existing); `XX` only updates existing members (never adds new); `CH` changes the return value from count-of-added to count-of-added-plus-changed; returns count of added members (or changed members with `CH`); AOF-logged |
| `GEODIST` | `GEODIST key member1 member2 [M \| KM \| FT \| MI]` | Return the distance between two members; default unit is metres (`M`); returns nil if either member is missing |
| `GEOPOS` | `GEOPOS key [member [member ...]]` | Return `[longitude, latitude]` pairs for each member; nil for missing members |
| `GEOHASH` | `GEOHASH key [member [member ...]]` | Return the standard 11-character base32 geohash string for each member; nil for missing members |

### Search commands

| Command | Syntax | Description |
|---------|--------|-------------|
| `GEOSEARCH` | `GEOSEARCH key <FROMMEMBER member \| FROMLONLAT longitude latitude> <BYRADIUS radius <M \| KM \| FT \| MI> \| BYBOX width height <M \| KM \| FT \| MI>> [ASC \| DESC] [COUNT count [ANY]] [WITHCOORD] [WITHDIST] [WITHHASH]` | Query members inside a circle or axis-aligned box; `FROMMEMBER` uses an existing member as the centre; `FROMLONLAT` uses explicit coordinates; `BYRADIUS` filters by great-circle distance; `BYBOX` filters by an axis-aligned bounding rectangle; results are sorted nearest-first by default; `WITHDIST` appends the distance in the requested unit; `WITHCOORD` appends the decoded `[lon, lat]`; `WITHHASH` appends the raw integer geohash score |
| `GEOSEARCHSTORE` | `GEOSEARCHSTORE destination source <FROMMEMBER member \| FROMLONLAT longitude latitude> <BYRADIUS radius <M \| KM \| FT \| MI> \| BYBOX width height <M \| KM \| FT \| MI>> [ASC \| DESC] [COUNT count [ANY]] [STOREDIST]` | Same search as `GEOSEARCH` but writes the results into `destination` as a sorted set; without `STOREDIST` the score is the geohash (so the destination is itself a valid geo index); with `STOREDIST` the score is the distance in the requested unit; returns the number of members stored; AOF-logged |

### Legacy commands

These commands are deprecated as of Redis 6.2 in favour of `GEOSEARCH` and `GEOSEARCHSTORE` but remain fully supported for client compatibility.

| Command | Syntax | Description |
|---------|--------|-------------|
| `GEORADIUS` | `GEORADIUS key longitude latitude radius <M \| KM \| FT \| MI> [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count [ANY]] [ASC \| DESC] [STORE key \| STOREDIST key]` | Query members within a radius of explicit coordinates; `STORE` writes results to a destination key as a geo index; `STOREDIST` writes with distance scores; AOF-logged when `STORE` or `STOREDIST` is used |
| `GEORADIUS_RO` | `GEORADIUS_RO key longitude latitude radius <M \| KM \| FT \| MI> [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count [ANY]] [ASC \| DESC]` | Read-only variant of `GEORADIUS`; does not support `STORE` or `STOREDIST` |
| `GEORADIUSBYMEMBER` | `GEORADIUSBYMEMBER key member radius <M \| KM \| FT \| MI> [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count [ANY]] [ASC \| DESC] [STORE key \| STOREDIST key]` | Query members within a radius of an existing member; supports `STORE` and `STOREDIST`; AOF-logged when storing |
| `GEORADIUSBYMEMBER_RO` | `GEORADIUSBYMEMBER_RO key member radius <M \| KM \| FT \| MI> [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count [ANY]] [ASC \| DESC]` | Read-only variant of `GEORADIUSBYMEMBER`; does not support `STORE` or `STOREDIST` |

---

## Stream commands

Stream commands operate on append-only log data structures. Reported as type `stream` by the `TYPE` command.

### Core commands

| Command | Syntax | Description |
|---------|--------|-------------|
| `XADD` | `XADD key [NOMKSTREAM] [KEEPREF \| DELREF \| ACKED] [IDMPAUTO producer-id \| IDMP producer-id idempotent-id] [MAXLEN\|MINID [=\|~] threshold [LIMIT count]] *\|id field value [...]` | Append an entry; `*` auto-generates ID as `ms-seq`; NOMKSTREAM skips creation if key absent; MAXLEN/MINID trim the stream after insert, and `KEEPREF` / `DELREF` / `ACKED` say what trimming does with entries consumer groups still reference. `IDMP` / `IDMPAUTO` (Redis 8.6) make the add idempotent: a retry by the same producer with the same id — or, with `IDMPAUTO`, the same content — adds nothing and returns the original entry's ID |
| `XLEN` | `XLEN key` | Return the number of entries |
| `XRANGE` | `XRANGE key start end [COUNT count]` | Return entries with IDs in `[start, end]`; `-` and `+` are open bounds; bare `ms` expands to `ms-0` / `ms-MAXSEQ` |
| `XREVRANGE` | `XREVRANGE key end start [COUNT count]` | Same as XRANGE but in reverse order |
| `XREAD` | `XREAD [COUNT count] [MAXCOUNT maxcount] [MAXSIZE maxsize] [BLOCK ms] STREAMS key [key ...] id [id ...]` | Read new entries across multiple streams; `$` means entries added after this call and `+` the last entry; BLOCK suspends until new data arrives or timeout. `COUNT` is per stream; `MAXCOUNT` and `MAXSIZE` (Redis 8.10) cap the entries and the bytes of the whole reply |
| `XDEL` | `XDEL key id [id ...]` | Delete entries by ID; returns count deleted |
| `XDELEX` | `XDELEX key [KEEPREF \| DELREF \| ACKED] IDS numids id [id ...]` | Delete entries with control over consumer-group references (Redis 8.2). Per id: `-1` no such entry, `1` deleted, `2` kept because a group still references it (`ACKED` only). `KEEPREF` (default) deletes and leaves pending-list references; `DELREF` deletes and removes them from every group; `ACKED` deletes only an entry every group has read and acknowledged. The policy may also follow the `IDS` block; `REF` is accepted as an older name for `ACKED` |
| `XTRIM` | `XTRIM key MAXLEN\|MINID [=\|~] threshold [LIMIT count] [KEEPREF \| DELREF \| ACKED]` | Trim stream by length or minimum ID. With `~` trimming is approximate, removing whole nodes of `stream-node-max-entries` entries, and `LIMIT` caps how many are removed (an error without `~`, as in Redis). `KEEPREF` (default) leaves consumer groups' pending references to trimmed entries, `DELREF` removes them, `ACKED` stops at the first entry not yet acknowledged by every group |
| `XSETID` | `XSETID key last-id [ENTRIESADDED n] [MAXDELETEDID id]` | Set the stream's last-generated ID and metadata |
| `XINFO STREAM` | `XINFO STREAM key [FULL [COUNT n]]` | Return stream metadata |

### Consumer group commands

| Command | Syntax | Description |
|---------|--------|-------------|
| `XGROUP CREATE` | `XGROUP CREATE key group id\|$ [MKSTREAM] [ENTRIESREAD n]` | Create a consumer group; `$` starts from the latest entry; MKSTREAM creates the key if absent |
| `XGROUP DESTROY` | `XGROUP DESTROY key group` | Delete a consumer group and its PEL |
| `XGROUP SETID` | `XGROUP SETID key group id\|$ [ENTRIESREAD n]` | Set the group's last-delivered ID, and optionally its entries-read counter |
| `XGROUP CREATECONSUMER` | `XGROUP CREATECONSUMER key group consumer` | Explicitly create a consumer |
| `XGROUP DELCONSUMER` | `XGROUP DELCONSUMER key group consumer` | Delete a consumer and remove its pending entries |
| `XREADGROUP` | `XREADGROUP GROUP group consumer [COUNT n] [MAXCOUNT n] [MAXSIZE n] [BLOCK ms] [CLAIM min-idle-time] [NOACK] STREAMS key [key ...] id [id ...]` | Read new (`>`) or pending (specific ID) entries for a consumer; NOACK skips PEL tracking. `CLAIM` (Redis 8.4) also takes over entries pending for other consumers that have been idle that long, ahead of new ones; every entry then carries its idle time and delivery count. A group that does not exist is a `NOGROUP` error |
| `XACK` | `XACK key group id [id ...]` | Acknowledge processed entries; removes from PEL |
| `XNACK` | `XNACK key group <SILENT \| FAIL \| FATAL> IDS numids id [id ...] [RETRYCOUNT count] [FORCE]` | Release pending entries back to the group PEL without acknowledging, for immediate re-delivery. SILENT decrements the delivery counter, FAIL leaves it, FATAL pins it to LLONG_MAX; RETRYCOUNT overrides. FORCE creates unowned entries for non-pending IDs that exist in the stream |
| `XPENDING` | `XPENDING key group [[IDLE ms] start end count [consumer]]` | Return pending entry summary or detail list |
| `XCLAIM` | `XCLAIM key group consumer min-idle-time id [id ...] [IDLE ms] [TIME ms] [RETRYCOUNT n] [FORCE] [JUSTID] [LASTID id]` | Claim ownership of pending entries idle longer than min-idle-time |
| `XAUTOCLAIM` | `XAUTOCLAIM key group consumer min-idle-time start [COUNT count] [JUSTID]` | Scan and claim idle pending entries starting from `start` |
| `XINFO GROUPS` | `XINFO GROUPS key` | List consumer groups with metadata |
| `XINFO CONSUMERS` | `XINFO CONSUMERS key group` | List consumers in a group with idle time and pending count |
| `XACKDEL` | `XACKDEL key group [KEEPREF \| DELREF \| ACKED] IDS numids id [id ...]` | Acknowledge and delete entries in one step (Redis 8.2). Per id: `-1` not pending in this group, `1` acknowledged and deleted (or already gone), `2` acknowledged but kept because another group still references it (`ACKED` only). Policies as for `XDELEX`. The bare form `XACKDEL key group id [id ...]` is still accepted |
| `XCFGSET` | `XCFGSET key field value [field value ...]` | Set per-stream configuration; supported fields: `MAX-ENTRIES n` (persistent trim policy applied on every XADD, `0` clears the policy), `MAX-DELETED-ENTRY-ID id` (advance the tracked max deleted ID, never moves backwards), `IDMP-DURATION seconds` and `IDMP-MAXSIZE n` (how long, and how many, idempotent ids are remembered per producer; defaults from `stream-idmp-duration` / `stream-idmp-maxsize`) |

A consumer-group read changes the group, so `XREADGROUP`, `XCLAIM` and
`XAUTOCLAIM` are replicated and logged as the changes they made (who owns
which entry, delivered how many times, when), not as the command: a replica's
pending lists match its master's exactly.

---

## Time Series commands (TS.*)

KiviDB implements the RedisTimeSeries command surface. Time series are stored natively as a sorted `BTreeMap<timestamp_ms, f64>` per key, giving O(log N) insert and O(k) range queries. A global label index enables O(matching keys) cross-series queries without full scans.

### Data lifecycle

| Command | Syntax | Description |
|---------|--------|-------------|
| `TS.CREATE` | `TS.CREATE key [RETENTION ms] [ENCODING COMPRESSED\|UNCOMPRESSED] [CHUNK SIZE size] [DUPLICATE_POLICY BLOCK\|FIRST\|LAST\|MIN\|MAX\|SUM] [LABELS label value ...]` | Create a new time series. ENCODING is accepted but ignored — storage is always BTreeMap. Without `DUPLICATE_POLICY`, a series (made here or by `TS.ADD`) takes `ts-duplicate-policy`: `last` unless set; RedisTimeSeries's default is `block` |
| `TS.ALTER` | `TS.ALTER key [RETENTION ms] [CHUNK SIZE size] [DUPLICATE_POLICY policy] [LABELS label value ...]` | Update retention, chunk size, duplicate policy, or labels. Updating labels atomically rebuilds the label index |
| `TS.DEL` | `TS.DEL key fromTimestamp toTimestamp` | Delete all samples in the inclusive timestamp range. Returns the count of deleted samples |

### Adding samples

| Command | Syntax | Description |
|---------|--------|-------------|
| `TS.ADD` | `TS.ADD key timestamp value [RETENTION ms] [ENCODING ...] [CHUNK SIZE size] [ON DUPLICATE policy] [LABELS label value ...]` | Append a sample. Use `*` for current time. Auto-creates the key if absent. Returns the stored timestamp |
| `TS.MADD` | `TS.MADD key timestamp value [key timestamp value ...]` | Append samples to one or more series in a single call. Auto-creates missing series. Returns an array of stored timestamps |
| `TS.INCRBY` | `TS.INCRBY key value [TIMESTAMP timestamp] [RETENTION ms] [CHUNK SIZE size] [LABELS label value ...]` | Add a new sample equal to the last sample's value plus the given increment. Creates the series at 0 if absent |
| `TS.DECRBY` | `TS.DECRBY key value [TIMESTAMP timestamp] [RETENTION ms] [CHUNK SIZE size] [LABELS label value ...]` | Add a new sample equal to the last sample's value minus the given decrement |

### Querying

| Command | Syntax | Description |
|---------|--------|-------------|
| `TS.GET` | `TS.GET key [LATEST]` | Return the sample with the highest timestamp. Returns empty array for an empty series, nil for a missing key |
| `TS.MGET` | `TS.MGET [LATEST] [WITHLABELS \| SELECTED_LABELS label ...] FILTER <filters>` | Return the latest sample from each series matching the label filters |
| `TS.RANGE` | `TS.RANGE key fromTimestamp toTimestamp [LATEST] [FILTER_BY_TS ts ...] [FILTER_BY_VALUE min max] [COUNT count] [[ALIGN value] AGGREGATION agg[,agg ...] bucketDuration [BUCKETTIMESTAMP bt] [EMPTY]]` | Query a range in forward (ascending) order. Several aggregators can be given, comma-separated (Redis 8.8): each row then carries one value per aggregator |
| `TS.REVRANGE` | `TS.REVRANGE key fromTimestamp toTimestamp [...]` | Query a range in reverse (descending) order. Accepts the same options as `TS.RANGE` |
| `TS.MRANGE` | `TS.MRANGE fromTimestamp toTimestamp [LATEST] [FILTER_BY_TS ts ...] [FILTER_BY_VALUE min max] [WITHLABELS \| SELECTED_LABELS label ...] [COUNT count] [[ALIGN value] AGGREGATION agg[,agg ...] bucketDuration] [EXCLUDEEMPTY] FILTER <filters> [GROUPBY label REDUCE reducer]` | Query a range across multiple series matching label filters, in forward order. `EXCLUDEEMPTY` (Redis 8.10) leaves out series with no samples in the range |
| `TS.MREVRANGE` | `TS.MREVRANGE fromTimestamp toTimestamp [...]` | Query a range across multiple series in reverse order. Accepts the same options as `TS.MRANGE` |
| `TS.INFO` | `TS.INFO key [DEBUG]` | Return metadata for a series: totalSamples, firstTimestamp, lastTimestamp, retentionTime, chunkSize, duplicatePolicy, labels, sourceKey (the series it is a compaction of, or nil), rules |
| `TS.QUERYINDEX` | `TS.QUERYINDEX <filters>` | Return all time series keys matching the given label filters. Results are sorted |
| `TS.NRANGE` | `TS.NRANGE numkeys key [key ...] fromTimestamp toTimestamp [same options as TS.RANGE]` | Query the same range over several named series and join the results by timestamp (Redis 8.10): one row per timestamp any of them reports, with a value — or `NaN` — for each key, in the order given |
| `TS.NREVRANGE` | `TS.NREVRANGE numkeys key [key ...] fromTimestamp toTimestamp [...]` | `TS.NRANGE` in reverse order |
| `TS.READ` | `TS.READ key timestamp [BLOCK milliseconds min_count] [MAX_COUNT max_count]` | The samples at or after a timestamp, oldest first (Redis 8.10) — for following a series. In place of a timestamp: `-` the earliest sample, `+` the latest (included), `$` only samples added from now on. With `BLOCK` it waits until `min_count` of them exist or the timeout passes |
| `TS.QUERYLABELS` | `TS.QUERYLABELS <LABELS \| VALUES label> [FILTER filter ...]` | The label names in use, or the values of one label, across the series matching the filters (Redis 8.10) |

### Compaction rules

| Command | Syntax | Description |
|---------|--------|-------------|
| `TS.CREATERULE` | `TS.CREATERULE sourceKey destKey AGGREGATION agg bucketDuration [alignTimestamp]` | Create a compaction rule from source to destination. A bucket is written to the destination once it has closed (when the first sample of a later bucket arrives), and again if a sample is added to it after; the bucket still open is reported only with `LATEST`, as in RedisTimeSeries. A destination has one source |
| `TS.DELETERULE` | `TS.DELETERULE sourceKey destKey` | Delete a compaction rule |

### Supported aggregation functions

`AVG`, `FIRST`, `LAST`, `MIN`, `MAX`, `SUM`, `RANGE` (max−min), `COUNT`, `STD.P` (population std dev), `STD.S` (sample std dev), `VAR.P`, `VAR.S`, `TWA` (time-weighted average, falls back to AVG), `COUNTNAN` (the NaN samples in the bucket), `COUNTALL` (every sample, NaN included).

A sample's value may be `NaN` (Redis 8.6). The numeric aggregators skip NaN samples; `COUNT` counts the others

### Label filter syntax

Used by `TS.QUERYINDEX`, `TS.MGET`, `TS.MRANGE`, `TS.MREVRANGE`:

| Filter | Meaning |
|--------|---------|
| `l=v` | Label equals value |
| `l!=v` | Label does not equal value |
| `l=` | Label is absent |
| `l!=` | Label is present |
| `l=(v1,v2,...)` | Label value is in the set |
| `l!=(v1,v2,...)` | Label value is not in the set |

### Design notes

- Retention is enforced on every `TS.ADD` using `BTreeMap::split_off` — O(log N), not O(N)
- The label index (`label=value → Set<key>`) is updated atomically on `TS.CREATE`, `TS.ALTER`, and `TS.ADD`. `TS.MRANGE` / `TS.QUERYINDEX` use it to seed candidates from the first `l=v` filter, then apply remaining filters in memory
- A global `SharedTsIndex` (separate from the main KV shards) is shared across all connections via `Arc<RwLock<_>>`
- `TS.ADD` auto-creates the series without a second shard lock: the `Arc<RwLock<TsInner>>` is cloned before the map lock is dropped

---

## Pub/Sub commands

KiviDB implements the full Redis Pub/Sub command surface including regular channels, glob-pattern subscriptions, and Redis 7.0 shard channels. Every subscribed connection has one queue: a publish appends to the queue of each subscriber, and the connection's own task writes it out. A subscriber therefore sees messages in the order they were published, across all of its channels and patterns, and none is ever dropped from the middle — a subscriber that falls more than 32 MB behind, or stays more than 8 MB behind for 60 seconds, is disconnected instead (Redis's `client-output-buffer-limit pubsub 32mb 8mb 60`). A publish with no subscribers takes no lock.

A RESP2 connection with subscriptions may run only the commands listed below; anything else is `ERR Can't execute '<command>': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context`. Once it has unsubscribed from everything it is an ordinary connection again. A RESP3 connection keeps every command while subscribed, and takes its messages as push frames. `CLIENT LIST` shows each connection's `sub=`, `psub=` and `ssub=` counts.

### Publisher commands (available in normal mode)

| Command | Syntax | Description |
|---------|--------|-------------|
| `PUBLISH` | `PUBLISH channel message` | Post a message to a channel. Delivered to all `SUBSCRIBE` subscribers and all `PSUBSCRIBE` subscribers whose pattern matches the channel. Returns the total number of clients that received the message |
| `SPUBLISH` | `SPUBLISH shardchannel message` | Post a message to a shard channel. Delivered only to `SSUBSCRIBE` subscribers — does not match patterns or regular subscriptions. Returns the number of shard subscribers that received it |

### Introspection commands (available in normal mode)

| Command | Syntax | Description |
|---------|--------|-------------|
| `PUBSUB CHANNELS` | `PUBSUB CHANNELS [pattern]` | Return the list of active channels (those with at least one subscriber). Optional glob pattern to filter |
| `PUBSUB NUMSUB` | `PUBSUB NUMSUB [channel ...]` | Return subscriber counts for the given channels as alternating `channel, count` pairs |
| `PUBSUB NUMPAT` | `PUBSUB NUMPAT` | Return the total number of active pattern subscriptions across all clients |
| `PUBSUB SHARDCHANNELS` | `PUBSUB SHARDCHANNELS [pattern]` | Return active shard channels. Optional glob pattern to filter |
| `PUBSUB SHARDNUMSUB` | `PUBSUB SHARDNUMSUB [shardchannel ...]` | Return subscriber counts for the given shard channels as alternating pairs |

### Subscriber commands

| Command | Syntax | Description |
|---------|--------|-------------|
| `SUBSCRIBE` | `SUBSCRIBE channel [channel ...]` | Subscribe to one or more channels. Each subscription sends an ack: `["subscribe", channel, total_count]`. Idempotent — subscribing to a channel you are already subscribed to does not create duplicate deliveries |
| `UNSUBSCRIBE` | `UNSUBSCRIBE [channel ...]` | Unsubscribe from channels. With no arguments, unsubscribes from all channels. Each unsubscription sends an ack: `["unsubscribe", channel, remaining_count]` |
| `PSUBSCRIBE` | `PSUBSCRIBE pattern [pattern ...]` | Subscribe to all channels matching a glob pattern. Messages are delivered as `["pmessage", pattern, channel, payload]` — note the 4-element format vs the 3-element `message` format |
| `PUNSUBSCRIBE` | `PUNSUBSCRIBE [pattern ...]` | Unsubscribe from patterns. With no arguments, unsubscribes from all patterns |
| `SSUBSCRIBE` | `SSUBSCRIBE shardchannel [shardchannel ...]` | Subscribe to one or more shard channels. Messages are delivered as `["smessage", shardchannel, payload]` |
| `SUNSUBSCRIBE` | `SUNSUBSCRIBE [shardchannel ...]` | Unsubscribe from shard channels. With no arguments, unsubscribes from all shard channels |
| `PING` | `PING [message]` | In subscriber mode returns `["pong", message]` (a 2-element array), not `+PONG`. This is the standard Redis subscriber-mode PING behaviour |
| `RESET` | `RESET` | Unsubscribe from everything and put the connection back as it was when it was opened: no transaction or watches, database 0, RESP2, no name, no tracking, and the `default` user if that needs no password |
| `QUIT` | `QUIT` | Close the connection cleanly |

### Message formats

```
Regular channel message (SUBSCRIBE / PUBLISH):
  *3
  $7 message
  $<channel_len> <channel>
  $<payload_len> <payload>

Pattern message (PSUBSCRIBE / PUBLISH):
  *4
  $8 pmessage
  $<pattern_len> <pattern>
  $<channel_len> <channel>        ← the actual channel published to, not the pattern
  $<payload_len> <payload>

Shard channel message (SSUBSCRIBE / SPUBLISH):
  *3
  $8 smessage
  $<shardchannel_len> <shardchannel>
  $<payload_len> <payload>
```

### Design notes

- A subscribed connection stays in its ordinary command loop. Under RESP3 it keeps every command; under RESP2 it is limited to the commands above until it has no subscriptions left.
- Every subscribed connection has one queue. A publish appends to the queue of each subscriber, and the connection's forwarder task writes from it to the socket, so a subscriber sees messages in the order they were published, whichever channel or pattern they came through.
- No message is skipped. A subscriber that cannot keep up is disconnected once its queue passes Redis's default `client-output-buffer-limit pubsub` (32 MB, or 8 MB for 60 seconds).

---

## Server commands

| Command | Syntax | Description |
|---------|--------|-------------|
| `PING` | `PING [message]` | With no argument returns `PONG`. With a message returns that message as a **bulk string** (Redis echo semantics). Subscriber mode still uses the Pub/Sub array reply |
| `ECHO` | `ECHO message` | Returns the message back |
| `SELECT` | `SELECT index` | Switch to a database (0–15) |
| `DBSIZE` | `DBSIZE` | Return the number of keys in the current database |
| `FLUSHDB` | `FLUSHDB` | Delete all keys in the current database |
| `FLUSHALL` | `FLUSHALL` | Delete all keys in all databases |
| `SAVE` | `SAVE` | Synchronously save a snapshot to `dump.kdb` |
| `BGSAVE` | `BGSAVE` | Save a snapshot in the background |
| `BGREWRITEAOF` | `BGREWRITEAOF` | Trigger a background AOF rewrite; returns `ERR` if AOF is not enabled |
| `LASTSAVE` | `LASTSAVE` | Return the Unix timestamp of the last successful save |
| `TIME` | `TIME` | Return `[unix_seconds, microseconds_within_second]` as two bulk strings; uses real wall clock |
| `HELLO` | `HELLO [protover [AUTH username password] [SETNAME clientname]]` | Switch the connection to RESP2 or RESP3 and return the server's details (`server`, `version`, `proto`, `id`, `mode`, `role`, `modules`) — a map under RESP3. `AUTH` and `SETNAME` apply before the reply. Unauthenticated, when a password is required, it is `NOAUTH` |
| `LOLWUT` | `LOLWUT [VERSION version]` | Return ASCII art and the server version |
| `INFO` | `INFO [section [section ...]]` | Return server info; sections include `server`, `keyspace`, `replication`, `errorstats` (per-error reply counters), `all`, etc. `INFO commandstats` has, for each command that has been called, `calls`, `usec`, `usec_per_call`, `rejected_calls` and `failed_calls`, and the Redis 8.8 slowlog fields `slowlog_count`, `slowlog_time_ms_sum` and `slowlog_time_ms_max`. `INFO memory` reports `used_memory` (allocated bytes — what `maxmemory` is compared with), `used_memory_rss`, `maxmemory_policy`, the slabs of string values (`value_slab_bytes`, and `value_slab_live_bytes` in them) and active defrag's `active_defrag_running`, `active_defrag_hits` (values moved) and `active_defrag_misses` (left, being held); `INFO stats` reports `evicted_keys` |
| `CONFIG GET` | `CONFIG GET parameter [parameter ...]` | Get a configuration parameter; supports `*` for all |
| `CONFIG SET` | `CONFIG SET param value [param value ...]` | All or nothing: an unknown parameter, a value out of range, a parameter that cannot change at run time or one named twice is an error naming it, and none of the pairs is applied. `requirepass`, `slowlog-log-slower-than`, `slowlog-max-len`, `maxmemory`, `maxmemory-policy`, `maxmemory-samples`, `lfu-log-factor`, `lfu-decay-time`, `activedefrag`, `active-defrag-ignore-bytes`, `active-defrag-threshold-lower`, `active-defrag-cycle-max`, `appendonly`, `loglevel`, `repl-backlog-size`, `repl-backlog-ttl`, `repl-diskless-load` all apply immediately at runtime without restart |
| `CONFIG REWRITE` | `CONFIG REWRITE` | Persist the current runtime config to the file the server was started with (default `/etc/kividb/kividb.conf`). Creates the file and parent directories if they don't exist. Atomic write via tmpfile-rename. Keeps the file's mode, and its owner when run as root; a new file is `0600`. Redis resets the mode to `0644` less the umask instead |
| `CONFIG RESETSTAT` | `CONFIG RESETSTAT` | Returns OK |
| `OBJECT ENCODING` | `OBJECT ENCODING key` | The encoding Redis would use for the value, from its size and the `*-max-listpack-*` / `set-max-intset-entries` settings: `int`, `embstr` or `raw` for strings; `listpack` or `quicklist` for lists; `listpack`, `intset` or `hashtable` for sets; `listpack` or `hashtable` for hashes; `listpack` or `skiplist` for sorted sets; `stream`. KiviDB stores each type one way internally; the name is for clients and tests that look at it |
| `OBJECT IDLETIME` | `OBJECT IDLETIME key` | Seconds since the key was last read or written, while an LRU or LRM `maxmemory-policy` is selected; an error under an LFU policy, as in Redis. `0` under the policies that do not go by use (`noeviction`, the random ones, `volatile-ttl`): keys are not watched then |
| `OBJECT FREQ` | `OBJECT FREQ key` | The key's logarithmic use count (Redis's LFU counter, with `lfu-log-factor` and `lfu-decay-time`) while an LFU `maxmemory-policy` is selected; an error otherwise, as in Redis |
| `OBJECT REFCOUNT` | `OBJECT REFCOUNT key` | Returns `1` for existing keys; nil for missing |
| `MEMORY USAGE` | `MEMORY USAGE key [SAMPLES count]` | Estimated bytes used by the key and its value; nil for a missing key |
| `MEMORY STATS` | `MEMORY STATS` | Memory figures as name–value pairs (`used_memory`, `used_memory_rss`, `used_memory_peak`, …) |
| `MEMORY DOCTOR` | `MEMORY DOCTOR` | A short report on memory health, in Redis's words |
| `MEMORY MALLOC-STATS` | `MEMORY MALLOC-STATS` | Allocator statistics (minimal) |
| `MEMORY PURGE` | `MEMORY PURGE` | Ask the allocator to return free memory; replies `OK` |
| `MEMORY HELP` | `MEMORY HELP` | List the `MEMORY` subcommands |
| `DEBUG` | `DEBUG ...` | Accepted and ignored for redis-cli compatibility |
| `QUIT` | `QUIT` | Close the connection |
| `ROLE` | `ROLE` | Return the replication role: `master` or `slave`, with offset and replica list |
| `REPLICAOF` | `REPLICAOF host port \| NO ONE` | Configure this instance as a replica, or promote to master. Refused in cluster mode, where `CLUSTER REPLICATE` does it |
| `SLAVEOF` | `SLAVEOF host port \| NO ONE` | Alias for `REPLICAOF` |
| `REPLCONF` | `REPLCONF ...` | Internal replication configuration command |
| `PSYNC` | `PSYNC repl_id offset` | Internal replication sync command |
| `SYNC` | `SYNC` | Old-style full synchronisation for a replica: the snapshot, then the command stream |
| `WAIT` | `WAIT numreplicas timeout` | Block until `numreplicas` replicas have acknowledged all preceding writes, or `timeout` milliseconds elapse; returns the number of replicas that acked |
| `WAITAOF` | `WAITAOF numlocal numreplicas timeout` | Block until the local AOF has fsynced all preceding writes, or `timeout` milliseconds elapse; returns `[local_fsynced, replica_fsynced]`, the first 1 once the write is fsynced. With the AOF off, a `numlocal` above 0 is an error and `WAITAOF 0 ...` replies `[0, 0]`, as in Redis |
| `SWAPDB` | `SWAPDB db1 db2` | Atomically swap the contents of two databases |
| `SHUTDOWN` | `SHUTDOWN [NOSAVE \| SAVE \| NOW \| FORCE \| ABORT]` | Gracefully stop the server. `NOSAVE` skips the final snapshot. `NOW` skips the AOF flush wait. `FORCE` ignores replica/AOF state. `ABORT` cancels a pending shutdown. Default (no modifier) writes a snapshot then exits |
| `MONITOR` | `MONITOR` | Stream every command executed on the server to this connection in real time. Format: `+<unix-ts.us> [<db> <addr>] "CMD" "arg1"`. Commands a script calls are shown with `lua` as the address, and a transaction as `MULTI`, its commands, `EXEC`. Administrative commands (`AUTH`, `CONFIG`, …) are not shown, as in Redis. `RESET` or `QUIT` leaves the mode. Zero cost when no monitors are connected (one atomic load per command) |

### CLIENT subcommands

| Command | Syntax | Description |
|---------|--------|-------------|
| `CLIENT SETNAME` | `CLIENT SETNAME name` | Set the connection name |
| `CLIENT GETNAME` | `CLIENT GETNAME` | Get the connection name |
| `CLIENT ID` | `CLIENT ID` | Return the client ID |
| `CLIENT INFO` | `CLIENT INFO` | Return info about the current connection |
| `CLIENT LIST` | `CLIENT LIST [TYPE normal\|master\|replica\|pubsub] [ID id [id ...]] [USER username] [ADDR ip:port] [LADDR ip:port] [SKIPME yes\|no] [MAXAGE seconds]` | Return one line per connection, in Redis's layout (`id`, `addr`, `laddr`, `name`, `age`, `idle`, `flags`, `db`, `sub`, `psub`, `ssub`, `multi`, `watch`, `cmd`, `user`, `redir`, `resp`, `lib-name`, `lib-ver`, …), filtered as asked |
| `CLIENT KILL` | `CLIENT KILL ip:port \| CLIENT KILL [ID id] [TYPE type] [USER username] [ADDR ip:port] [LADDR ip:port] [SKIPME yes\|no] [MAXAGE seconds]` | Close the matching connections. The old form (`ip:port`) replies `OK` or `ERR No such client`; the filter form replies with the number killed. `SKIPME` defaults to `yes`; with `no` the caller is killed too, after it has been sent the reply. Killed connections get no reply |
| `CLIENT NO-EVICT` | `CLIENT NO-EVICT ON\|OFF` | Mark or unmark the connection as non-evictable |
| `CLIENT NO-TOUCH` | `CLIENT NO-TOUCH ON\|OFF` | Accepted; has no effect — key access times are recorded only while an LRU/LFU `maxmemory-policy` is selected, and not per client |
| `CLIENT PAUSE` | `CLIENT PAUSE timeout [WRITE \| ALL]` | Pause clients for `timeout` milliseconds, until it expires or `CLIENT UNPAUSE`. `ALL` (the default) holds every command but `CLIENT UNPAUSE`; `WRITE` holds anything that may write — writes, `PFCOUNT`, `PUBLISH`, scripts not declared `no-writes`, and an `EXEC` containing any of them. A pause given during another keeps the more restrictive of the two. While writes are paused nothing expires, as in Redis: a read takes a key whose time has passed as gone, but it is deleted, counted in `expired_keys` and announced only once the pause is over, and `RANDOMKEY` may name it rather than answer nil. The same holds during a slot handoff and a manual failover |
| `CLIENT UNPAUSE` | `CLIENT UNPAUSE` | Release all connections blocked by `CLIENT PAUSE` immediately |
| `CLIENT CACHING` | `CLIENT CACHING yes\|no` | `yes` is accepted only in `OPTIN` mode and `no` only in `OPTOUT` mode, with Redis's errors otherwise. Set a single-shot opt-in or opt-out flag for the next data read command. In OPTIN mode: `yes` registers the next read for invalidation tracking. In OPTOUT mode: `no` suppresses registration for the next read. The flag is consumed by exactly one data command then cleared. Has no effect outside OPTIN/OPTOUT mode |
| `CLIENT TRACKING` | `CLIENT TRACKING ON\|OFF [REDIRECT client-id] [BCAST] [PREFIX prefix [...]] [OPTIN] [OPTOUT] [NOLOOP]` | Enable or disable client-side cache invalidation. Invalidations reach a RESP3 connection as push messages; a RESP2 connection has to `REDIRECT` them to a connection subscribed to `__redis__:invalidate`, and without `REDIRECT` is sent nothing, as in Redis. Every key a command reads is tracked, and keys written by scripts, expired, evicted or flushed are invalidated. Default mode: all reads auto-register; writers receive their own invalidations. OPTIN: only reads preceded by `CLIENT CACHING yes` are registered. OPTOUT: all reads register unless preceded by `CLIENT CACHING no`. NOLOOP: suppress invalidations sent back to the client that caused the write. REDIRECT forwards invalidations to another client ID. Zero cost when no tracking clients connected — one atomic load per command on the hot path |
| `CLIENT TRACKINGINFO` | `CLIENT TRACKINGINFO` | Return tracking state for the current connection: flags (off, on, bcast, optin, optout, noloop), redirect client ID (-1 if none), and prefix list |
| `CLIENT SETINFO` | `CLIENT SETINFO LIB-NAME name \| LIB-VER ver` | Store the client library name and version on this connection; shown by `CLIENT LIST` and `CLIENT INFO` as `lib-name` and `lib-ver` |
| `CLIENT UNBLOCK` | `CLIENT UNBLOCK id` | Returns 0; accepted for compatibility, not functional |
| `CLIENT GETREDIR` | `CLIENT GETREDIR` | Return the client ID this connection redirects invalidations to, or -1 if no redirect is set |
| `CLIENT REPLY` | `CLIENT REPLY ON\|OFF\|SKIP` | `OFF` stops replies on this connection until `ON`; `SKIP` drops the reply to the next command. Messages — Pub/Sub, RESP3 pushes — are still sent |

### COMMAND subcommands

| Command | Syntax | Description |
|---------|--------|-------------|
| `COMMAND` | `COMMAND` | Every command, each as `[name, arity, [flags], first-key, last-key, key-step, [@categories], [], [], []]`. The three key numbers say where a command's keys are (a negative `last-key` counts from the end); a command whose keys depend on its arguments has the `movablekeys` flag, and `COMMAND GETKEYS` answers for it. This is what redis-py's cluster client routes by. The arity is a lower bound |
| `COMMAND COUNT` | `COMMAND COUNT` | Return the number of supported commands |
| `COMMAND LIST` | `COMMAND LIST [FILTERBY MODULE module \| ACLCAT category \| PATTERN pattern]` | Return all command names; optionally filter by module (always empty), ACL category, or glob pattern |
| `COMMAND GETKEYS` | `COMMAND GETKEYS command [arg ...]` | Return the key names from a command invocation |
| `COMMAND GETKEYSANDFLAGS` | `COMMAND GETKEYSANDFLAGS command [arg ...]` | Return key names with read/write flags per key |
| `COMMAND DOCS` | `COMMAND DOCS [command ...]` | Documentation for the named commands, or for every top-level command (subcommands nested under `subcommands`) when none are named: each entry is a map of `summary`, `since`, `group`, `complexity`, `history`, `doc_flags` and `arguments` (plus `module` for search, JSON, time series and vector set commands, and `deprecated_since` / `replaced_by` where they apply). A name that is not a command is omitted. RESP2 uses a flat array of those pairs; RESP3 a map. `COMMAND` / `COMMAND INFO` still carry arity and key positions |
| `COMMAND INFO` | `COMMAND INFO [command ...]` | The same entries for the named commands (nil for a name that is not a command), or for all of them |

---

## ACL commands

KiviDB implements Redis 6's ACLs, with several users. The `default` user always exists and cannot be deleted. Additional users can be created, stored, and queried. Command permission rules are enforced on every connection.

### Authentication

| Command | Syntax | Description |
|---------|--------|-------------|
| `AUTH` | `AUTH [username] password` | Authenticate the connection; `AUTH password` (legacy) or `AUTH username password` (Redis 6 ACL form); required when `--requirepass` is set or when the user has a password; `PING`, `AUTH`, `QUIT`, `HELLO`, and `RESET` are always allowed before authentication. On success returns **`OK` as a bulk string** (`$2\r\nOK\r\n`), matching Redis—not a simple-string `+OK` |

### User management

| Command | Syntax | Description |
|---------|--------|-------------|
| `ACL WHOAMI` | `ACL WHOAMI` | Return the username of the current authenticated connection |
| `ACL LIST` | `ACL LIST` | Return all users in ACL file format: `user <name> <on\|off> <nopass\|#hash> ~keys &channels +commands` |
| `ACL USERS` | `ACL USERS` | Return a list of all usernames |
| `ACL GETUSER` | `ACL GETUSER username` | Return the rules for a user as a flat key-value array (`flags`, `passwords`, `commands`, `keys`, `channels`, `selectors`); nil if user not found |
| Key and channel patterns | `~pattern`, `%R~pattern`, `%W~pattern`, `%RW~pattern`, `&pattern` | Enforced on every command: `%R~` allows reading the matching keys only, `%W~` writing only, `~` and `%RW~` both. A user keeps every pattern it is given. Channel patterns (`&`) are enforced too: a user with none cannot publish or subscribe. Patterns use Redis's glob rules |
| `ACL DELUSER` | `ACL DELUSER username [username ...]` | Delete users; returns count deleted; the `default` user cannot be deleted |

### Introspection

| Command | Syntax | Description |
|---------|--------|-------------|
| `ACL CAT` | `ACL CAT [category]` | Without argument: return all ACL category names; with argument: return all commands in that category |
| `ACL LOG` | `ACL LOG [count \| RESET]` | Return recent ACL security events (auth failures, NOPERM violations); `RESET` clears the log; optional count limits entries returned |
| `ACL DRYRUN` | `ACL DRYRUN username command [arg ...]` | Simulate running a command as the given user; returns `OK` if permitted or a description of the denial |

### Utilities

| Command | Syntax | Description |
|---------|--------|-------------|
| `ACL GENPASS` | `ACL GENPASS [bits]` | Generate a cryptographically random hex password, a digit per 4 bits; `bits` defaults to 256 and may be 1 to 4096 (rounded up to a multiple of 4: `ACL GENPASS 5` gives two digits) |
| `ACL SAVE` | `ACL SAVE` | Save all users to the configured ACL file (`--aclfile`); returns `ERR` if no ACL file is configured |
| `ACL LOAD` | `ACL LOAD` | Reload users from the configured ACL file; returns `ERR` if no ACL file is configured |

### ACL file persistence

Set the ACL file path at startup:
```bash
kividb --aclfile /etc/kividb/users.acl
```

The file uses one line per user in standard ACL format:
```
user default on nopass ~* &* +@all
user alice on #5e884898da28047151d0e56f8dc6292773603d0d6aabbdd62a11ef721d1542d8 ~* &* +@string +@hash
```

`ACL SETUSER` and `ACL DELUSER` persist changes to the file immediately when `--aclfile` is configured.

### Command permission enforcement

After authentication, every command is checked against the authenticated user's permission rules before execution. Denied commands return:
```
-NOPERM User <name> has no permissions to run the '<command>' command
```

Rules are applied left to right; the last matching rule wins. Example rule sets:

| Rule string | Effect |
|-------------|--------|
| `+@all` | Full access to all commands |
| `-@all +@string +@hash` | Only string and hash commands |
| `-@all +get +set` | Only GET and SET |
| `+@all -@dangerous` | Everything except what can destroy data, stall the server or reconfigure it |
| `+@all -@admin +config\|get` | No administration, except reading the configuration |
| `-@all +@read` | Read-only, across every data type |

Categories (`ACL CAT`): the data types `string` `list` `set` `sortedset` `hash`
`bitmap` `hyperloglog` `geo` `stream` `array` `json` `search` `timeseries`
`vector`; `keyspace` / `generic`; `read` and `write`; `fast` and `slow`;
`blocking`; `pubsub`, `scripting`, `transaction`, `connection`, `cluster`,
`server`; and `admin` and `dangerous`.

A command with subcommands can be allowed and denied per subcommand, written
`command|subcommand`: `-client|kill`, `+config|get`. A rule naming the command
covers all of its subcommands. `admin` and `dangerous` are per subcommand where
it matters: `CLUSTER RESET`, `CLUSTER FORGET`, `CLUSTER MIGRATION` and
`CLIENT KILL` are in them, while `CLUSTER SLOTS`, `CLUSTER SHARDS`,
`CLIENT SETNAME` and `CLIENT SETINFO` — which client libraries call on connect
— are not.

A change to a user (`ACL SETUSER`, `ACL DELUSER`, `ACL LOAD`) applies to that
user's open connections from their next command; a deleted user's connections
are closed. Commands already queued in a `MULTI` are checked against the rules
in force when `EXEC` runs them.

---

## Transaction commands

| Command | Syntax | Description |
|---------|--------|-------------|
| `MULTI` | `MULTI` | Begin a transaction; subsequent commands are queued and return `+QUEUED` |
| `EXEC` | `EXEC` | Execute all queued commands atomically; returns nil if a `WATCH`ed key was modified |
| `DISCARD` | `DISCARD` | Discard all queued commands and exit transaction mode |
| `WATCH` | `WATCH key [key ...]` | Mark keys for optimistic locking; must be called before `MULTI` |
| `UNWATCH` | `UNWATCH` | Clear all watched keys for the current connection |

### Design notes

- Errors within `EXEC` are returned inline — a failed command does not abort the rest of the transaction
- `WATCH` uses a global registry with per-connection `Arc<AtomicBool>` dirty flags — checking is a single atomic load at `EXEC` time, zero overhead when no transaction is active
- Nesting `MULTI` inside an active transaction returns `ERR MULTI calls can not be nested`
- `WATCH` inside an active `MULTI` returns an error
- A write dirties a `WATCH` on any of the keys it names — both keys of `RENAME`, every key of `MSET` — as do `FLUSHDB`, `FLUSHALL`, `SWAPDB` and writes made from scripts
- In cluster mode a command is routed when it is queued: one for another node's slot is answered with `-MOVED` there and then and the transaction is aborted (`EXEC` replies `-EXECABORT`). At `EXEC` the queued commands are routed again and must all address one slot (`-CROSSSLOT` otherwise). A transaction therefore never runs in part
- `EXEC` does not isolate its commands from other clients' reads — see [`docs/EXEC_ATOMICITY.md`](docs/EXEC_ATOMICITY.md)

---

## Cluster

KiviDB supports Redis Cluster as a first-class feature. Cluster mode is opt-in
via `--cluster-enabled yes`; standalone mode is the default and is unaffected
by anything in this section.

### Topology and persistence

Cluster topology (node IDs, peer addresses, slot ownership, epochs, last-vote
epoch) is persisted to `nodes.conf` in the working directory. The format is
deliberately compatible with operator expectations from Redis's own
`nodes.conf`. Saves are atomic via tmpfile-rename.

The cluster bus listens on `client_port + 10000` (the standard Redis convention).
Bus protocol is RESP-based — KiviDB nodes can cluster with each other natively;
interop with vanilla `redis-server` would require implementing Redis's binary
cluster bus format.

Without `cluster-bus-secret`, anything that can reach the bus port can join
the cluster. With the same secret on every node, a node must prove it knows it
(an HMAC-SHA256 answer to a random challenge; the secret is not sent) before
anything it sends on the bus is read. `--cluster-bus-port-protected-mode yes`
starts a node that has a secret and refuses one that has not; the default
starts and logs a warning. Without `tls-cluster`, what nodes send each other is
in the clear, as in Redis: keep the bus on a private network, or turn on
`tls-cluster` (builds with TLS). A `nodes.conf` that exists but cannot be read is
fatal at startup rather than silently replaced by a new identity.

A cluster node has one keyspace: `SELECT` of any database but 0 is refused.

| Command | Syntax | Notes |
|---|---|---|
| `CLUSTER INFO` | `CLUSTER INFO` | Cluster state, slot counts, known nodes, current/my epoch, gossip stats. |
| `CLUSTER MYID` | `CLUSTER MYID` | Returns this node's 40-hex ID. Stable across restarts. |
| `CLUSTER MYSHARDID` | `CLUSTER MYSHARDID` | Returns the shard ID — own ID for masters, master's ID for replicas. |
| `CLUSTER NODES` | `CLUSTER NODES` | Full node table as a bulk string. Format matches Redis. |
| `CLUSTER SLOTS` | `CLUSTER SLOTS` | Slot-to-node mapping. Each entry: `[start, end, [master_ip, port, id], ...replicas]`. |
| `CLUSTER SHARDS` | `CLUSTER SHARDS` | Per-shard summary including slots and node descriptions. |
| `CLUSTER KEYSLOT` | `CLUSTER KEYSLOT key` | CRC16/XMODEM mod 16384 with `{tag}` extraction. |
| `CLUSTER COUNTKEYSINSLOT` | `CLUSTER COUNTKEYSINSLOT slot` | Walks the current DB to count keys hashing to `slot`. |
| `CLUSTER GETKEYSINSLOT` | `CLUSTER GETKEYSINSLOT slot count` | Returns up to `count` keys in `slot`. |

### Slot ownership

| Command | Syntax | Notes |
|---|---|---|
| `CLUSTER ADDSLOTS` | `CLUSTER ADDSLOTS slot [slot ...]` | Claim individual slots. Atomic — rejects duplicates. |
| `CLUSTER ADDSLOTSRANGE` | `CLUSTER ADDSLOTSRANGE start end [start end ...]` | Claim contiguous ranges. |
| `CLUSTER DELSLOTS` | `CLUSTER DELSLOTS slot [slot ...]` | Release individual slots. |
| `CLUSTER DELSLOTSRANGE` | `CLUSTER DELSLOTSRANGE start end [start end ...]` | Release contiguous ranges. |
| `CLUSTER FLUSHSLOTS` | `CLUSTER FLUSHSLOTS` | Clear all slot ownership for this node. |
| `CLUSTER SETSLOT` | `CLUSTER SETSLOT slot <IMPORTING\|MIGRATING\|NODE> node-id` or `CLUSTER SETSLOT slot STABLE` | Mark migration state or finalize ownership transfer. |

### Discovery and membership

| Command | Syntax | Notes |
|---|---|---|
| `CLUSTER MEET` | `CLUSTER MEET ip port [cluster-bus-port]` | Initiate handshake with a peer. Bus port defaults to `port + 10000`. A peer that cannot be reached leaves no entry behind. |
| `CLUSTER FORGET` | `CLUSTER FORGET node-id` | Remove a node from the routing table. Slots owned by the forgotten node become unassigned. The node is refused re-admission for 60 seconds, so gossip from a node that has not been told yet does not bring it back. |
| `CLUSTER LINKS` | `CLUSTER LINKS` | Report active bus links to peers. |
| `CLUSTER REPLICAS` / `CLUSTER SLAVES` | `CLUSTER REPLICAS master-id` | The replicas of a master: an array with one `CLUSTER NODES` line per replica (empty for a master without any). Aliases. |
| `CLUSTER COUNT-FAILURE-REPORTS` | `CLUSTER COUNT-FAILURE-REPORTS node-id` | Number of distinct nodes currently flagging this node as failing. Excludes the local node's own opinion. |

### Replication and failover

KiviDB cluster failover uses an epoch-voting algorithm: replicas of a `FAIL`'d
master race to claim its slots; each replica bumps the cluster's
`current_epoch`, requests votes from other masters, and on quorum (majority of
remaining masters) takes over. Higher-epoch claims always win during gossip
convergence, so even a recovering old master will accept its replacement.

| Command | Syntax | Notes |
|---|---|---|
| `CLUSTER REPLICATE` | `CLUSTER REPLICATE master-id` | Configure this node as a replica. Refuses if the candidate owns slots — `DELSLOTS` first. |
| `CLUSTER FAILOVER` | `CLUSTER FAILOVER [FORCE \| TAKEOVER]` | Manually promote this replica. By default the master pauses writes and the replica catches up before it stands for election, so nothing acknowledged is lost. `FORCE` skips that handshake (the master may be unreachable) but still wins an election; `TAKEOVER` skips the election too (dangerous). |

Automatic failover triggers when a master is `FAIL`-flagged via the failure
detection algorithm: `node-timeout` (default 15000ms, configurable via
`--cluster-node-timeout`) without a PONG marks a peer `PFAIL` (local opinion);
when a quorum of masters concur via gossip, the flag escalates to `FAIL`
(cluster opinion).

When a master fails, its replicas do not race blindly: each asks the others
how far it has read, and waits an extra second for every one that has read
further, so the most up-to-date replica normally stands first and wins, as in
Redis. A replica whose link to its master has been down for longer than
`cluster-node-timeout × cluster-replica-validity-factor` plus
`repl-ping-replica-period` does not stand at all: its data is too old to take
over with (`cluster-replica-validity-factor 0` lets it stand anyway).
`CLUSTER FAILOVER` is not subject to either.

A master that comes back after it has been replaced finds its slots claimed at
a higher epoch and becomes a replica of the node that took them. While a slot
is uncovered, or this node cannot see a majority of masters, keyed commands
are answered with `-CLUSTERDOWN` (`--cluster-require-full-coverage no` keeps
serving the slots that are covered).

### Migration

Two ways to move slots between masters.

**Atomic slot migration** (Redis 8.4) moves whole slot ranges in one operation,
run on the node that is to receive them:

| Command | Syntax | Notes |
|---|---|---|
| `CLUSTER MIGRATION IMPORT` | `CLUSTER MIGRATION IMPORT start-slot end-slot [start-slot end-slot ...]` | Import the ranges from the node that owns them (one source per command). Returns a task ID at once; the migration runs in the background. |
| `CLUSTER MIGRATION STATUS` | `CLUSTER MIGRATION STATUS [ID task-id \| ALL]` | The tasks this node knows of, as source or destination: `id`, `slots`, `source`, `dest`, `operation` (`import` / `migrate`), `state` (`in_progress`, `completed`, `failed`, `canceled`), `last_error`, `retries`, the three timestamps and `write_pause_ms`. |
| `CLUSTER MIGRATION CANCEL` | `CLUSTER MIGRATION CANCEL <ID task-id \| ALL>` | Cancel; returns the number of tasks cancelled. The source keeps the slots and the destination drops what it had received. |

The source sends a snapshot of the slots' keys and then the writes that arrive
for them meanwhile. When the destination is nearly caught up
(`--cluster-slot-migration-handoff-max-lag-bytes`, default 1 MiB) the source
pauses writes, sends the rest, and the destination takes ownership with a
single epoch bump; the source then deletes its copy. Clients see no `-ASK` and
no `-TRYAGAIN` — only, at the end, the `-MOVED` that tells them where the slots
now are — and no acknowledged write is lost or applied twice. The pause is
bounded by `--cluster-slot-migration-write-pause-timeout` (default 10000 ms),
after which the attempt is abandoned and retried. During the pause all client
writes to the source wait, not only those for the moving slots.

Until the takeover, what the destination has received is a copy: the source
still owns and serves the slots. If the destination goes down before taking
them over, the import is over (tasks are kept in memory), and when it starts
again it deletes the keys it had received in slots it does not own. It knows
which imports were under way from `<cluster-config-file>.imports`, which it
keeps next to its cluster config. The takeover itself is saved to the cluster
config before the source is told, and if that save fails the slots stay with
the source. Keys moved here with `MIGRATE`, key by key, are left alone: the
source deleted them as it sent them.

**The classical workflow**, which `redis-cli --cluster reshard` and
`rebalance` drive:

1. `CLUSTER SETSLOT <slot> IMPORTING <source-id>` on destination
2. `CLUSTER SETSLOT <slot> MIGRATING <dest-id>` on source
3. `MIGRATE <dest-host> <dest-port> "" 0 <timeout> KEYS key [key ...]` on source, for batches of the slot's keys
4. `CLUSTER SETSLOT <slot> NODE <dest-id>` on destination, then on source

During migration:
- Source serves keys it still has; returns `-ASK <slot> <dest>` for keys
  already migrated
- Destination accepts commands for the slot only when the client has issued
  `ASKING` on the connection
- `ASKING` is single-shot — it bypasses the importing-without-ownership check
  exactly once
- A multi-key command with some of its keys on each side is answered with
  `-TRYAGAIN`

| Command | Syntax | Notes |
|---|---|---|
| `MIGRATE` | `MIGRATE host port key\|"" db timeout [COPY] [REPLACE] [AUTH password \| AUTH2 username password] [KEYS key [key ...]]` | Serialise each key, `RESTORE` it on the destination with its TTL, and delete it here once the destination has acknowledged. Every data type that `DUMP` supports. |
| `ASKING` | `ASKING` | Per-connection single-shot flag. Required before each query against an `IMPORTING` slot. |
| `READONLY` | `READONLY` | Per-connection flag: read commands for slots this node replicates are served here instead of redirected to the master. |
| `READWRITE` | `READWRITE` | Clears the `READONLY` flag. |

A node that loses a slot to another master without having migrated its keys —
a bare `SETSLOT … NODE` on the other side, or the losing side of a partition —
deletes the keys it holds for that slot, as Redis does.

### Epoch and lifecycle

| Command | Syntax | Notes |
|---|---|---|
| `CLUSTER BUMPEPOCH` | `CLUSTER BUMPEPOCH` | Advance `current_epoch` and return `BUMPED <new>`. |
| `CLUSTER SET-CONFIG-EPOCH` | `CLUSTER SET-CONFIG-EPOCH epoch` | Set this node's `config_epoch`. Only valid when current value is 0. |
| `CLUSTER SAVECONFIG` | `CLUSTER SAVECONFIG` | Force a write of `nodes.conf`. |
| `CLUSTER RESET` | `CLUSTER RESET [HARD \| SOFT]` | Clear topology: forget the other nodes and the slot assignments. `SOFT` (default) keeps the node ID and epochs, and is refused on a master that holds keys; `HARD` also resets the epochs, flushes the dataset and gives the node a new ID. |

### Statistics

| Command | Syntax | Notes |
|---|---|---|
| `CLUSTER SLOT-STATS` | `CLUSTER SLOT-STATS SLOTSRANGE start end` or `CLUSTER SLOT-STATS ORDERBY metric [LIMIT n] [ASC \| DESC]` | Per-slot stats. Only `key-count` carries real data; `cpu-usec` / `network-bytes-in` / `network-bytes-out` are reported as 0 (no per-slot instrumentation yet). `ORDERBY` only accepts `key-count`, and sorts descending (the busiest slots first) unless `ASC` is given, as in Redis. |

### Configuration flags

| Flag | Default | Purpose |
|---|---|---|
| `--cluster-enabled yes\|no` | `no` | Activate cluster mode. Standalone otherwise. |
| `--cluster-node-timeout <ms>` | `15000` | PFAIL threshold. Lower values detect faster, increase false positives under jitter. |
| `--cluster-config-file <path>` | `nodes.conf` | Where the topology is persisted. |
| `--cluster-require-full-coverage yes\|no` | `yes` | Stop serving keyed commands while any slot is unassigned or its owner has failed. |
| `--cluster-replica-validity-factor <n>` | `10` | A replica out of touch with its master for longer than this many node timeouts (plus `repl-ping-replica-period`) does not stand for election when the master fails. `0`: always stand. Settable at runtime. |
| `--cluster-announce-ip <addr>` | (observed) | The address this node tells the cluster to reach it on — for NAT, containers and Kubernetes. |
| `--cluster-announce-port <port>` / `--cluster-announce-bus-port <port>` | client port / +10000 | The ports announced with it. |
| `--cluster-announce-client-port <port>` | `0` (the client port) | The port *clients* are told: in `MOVED`/`ASK` and `CLUSTER SLOTS`, `SHARDS` and `NODES`. Nodes keep reaching each other at the client port. For a NodePort, or a load balancer that maps ports. Settable at runtime. |
| `--cluster-bus-port-protected-mode yes\|no` | `no` | Refuse to start as a cluster node while the bus is unauthenticated. |
| `--cluster-slot-migration-handoff-max-lag-bytes <n>` | `1048576` | Atomic slot migration: how close the destination must be before writes are paused for the handoff. |
| `--cluster-slot-migration-write-pause-timeout <ms>` | `10000` | Atomic slot migration: the longest writes stay paused for a handoff. |

---

## Persistence

KiviDB supports two persistence mechanisms:

**RDB snapshots** — triggered manually:
```bash
SAVE      # blocking
BGSAVE    # background
LASTSAVE  # Unix timestamp of last successful save
```

Snapshots are written to `dump.kdb`. KiviDB also loads `dump.rdb` (Redis RDB format) at startup if `dump.kdb` is absent.

**AOF (Append-Only File):**
```bash
kividb --aof
BGREWRITEAOF   # trigger background rewrite
```

Write commands are appended to `appendonly.aof` and flushed every second.

A TTL is logged — and replicated — as the moment the key expires, not as a
duration: `SET k v EX 100` is recorded as `SET k v PXAT <unix-ms>`, `EXPIRE`
as `PEXPIREAT`, `HEXPIRE` as `HPEXPIREAT`, and so on. A key therefore expires
when it was meant to, however much later the log is replayed.

**Online backup** (Redis 8.10):

| Command | Description |
|---------|-------------|
| `BACKUP START` | Open a backup: write a BASE snapshot of the dataset as of this instant into the backup directory, and start collecting the writes that follow |
| `BACKUP LIST` | The files that are complete and safe to copy: BASE; after `SEAL`, also INCR and the manifest |
| `BACKUP SEAL` | Freeze the backup: it now holds everything up to this moment |
| `BACKUP STATUS` | `state`, `error`, start and end time |
| `BACKUP ABORT` | Cancel a backup that has not been sealed |
| `BACKUP CLEANUP` | Remove a sealed or failed backup's files |

A sealed backup is three files — `appendonly.aof.N.base.kdb`,
`appendonly.aof.N.incr.aof` and `appendonly.aof.manifest` — in
`--backupdirname` (default `backupdir`, under the working directory).
Restoring it gives the dataset as it was at the seal, not at the start, so the
nodes of a cluster can be started one at a time and sealed together. It works
with AOF on or off, on a master or a replica. `--backup-sealed-ttl <seconds>`
cleans a sealed backup up automatically (0, the default, keeps it).

To start a server from a backup:

```bash
kividb --preload-file aof:/path/to/appendonly.aof.manifest   # a sealed backup
kividb --preload-file rdb:/path/to/dump.kdb                  # a single snapshot
```

The file is loaded instead of the usual `dump.kdb` and `appendonly.aof`; a
target that cannot be loaded is fatal. See [`docs/PERSISTENCE.md`](docs/PERSISTENCE.md).

---

## Replication

```bash
kividb --replicaof 127.0.0.1 6379   # start as replica
REPLICAOF NO ONE                     # promote to master at runtime
```

Full resync sends a KDB snapshot followed by a live command stream.

| Option | Default | Purpose |
|---|---|---|
| `--masterauth <password>` / `--masteruser <user>` | | Credentials the replica presents to a master with `requirepass` or ACLs |
| `--repl-timeout <seconds>` | `60` | A replica declares the link dead after this long without a byte from the master; also bounds each handshake step |
| `--repl-ping-replica-period <seconds>` | `10` | How often the master puts a `PING` on an idle replication stream |
| `--replica-full-sync-guard off\|empty\|shrink` | `empty` | Refuse a full resync that would replace this replica's dataset with an empty one (`empty`), or with one below `--replica-full-sync-guard-ratio` percent of its key count (`shrink`) |

`INFO replication` on a master reports `connected_slaves`, one
`slaveN:ip=…,port=…,state=online,offset=…,lag=…` line per replica (`offset` is
what that replica has acknowledged, `lag` the seconds since it last did),
`master_replid` and `master_repl_offset`. On a replica it reports
`master_link_status` — `up` only when the replica is synced and receiving —
`master_last_io_seconds_ago`, `master_link_down_since_seconds` while the link
is down, `master_sync_in_progress`, `slave_repl_offset`, and the guard's state
(`replica_full_sync_guard`, `replica_full_syncs_refused`,
`replica_last_sync_refusal`).

A replica loads its own `dump.kdb` and AOF at startup before it contacts its
master. What reaches a replica is the *effect* of each write: a random pop as
the members it removed, a relative TTL as the moment of expiry, a
consumer-group read as the pending-list changes it made.

---

## Pattern matching

`KEYS`, `SCAN`, `HSCAN`, `SSCAN`, `ZSCAN`, and `PUBSUB CHANNELS` support glob-style patterns:

| Pattern | Matches |
|---------|---------|
| `*` | Any sequence of characters |
| `?` | Any single character |
| `h?llo` | `hello`, `hallo`, `hxllo` |
| `h*llo` | `hllo`, `heeeello` |
| `h[ae]llo` | `hello`, `hallo` |

---

## Error responses

| Prefix | Meaning |
|--------|---------|
| `ERR` | General error — wrong argument count, invalid value, syntax error |
| `WRONGTYPE` | Command used against a key holding the wrong data type |
| `NOAUTH` | Command requires authentication |
| `WRONGPASS` | Invalid username-password pair |
| `NOPERM` | Authenticated user lacks permission to run the command |
| `BUSYKEY` | `RESTORE` target key exists and `REPLACE` was not specified |
| `ERR Protocol error: …` | What the client sent cannot be a request — a count or length that is not a number, an argument over 512 MB, a line over 64 KB, unbalanced quotes. The connection is closed after this reply |
| `BUSY` | A script has been running for longer than `lua-time-limit`; only `SCRIPT KILL`, `FUNCTION KILL` and `SHUTDOWN NOSAVE` are accepted until it ends |

---

## Scripting commands (Lua)

Lua scripting (`EVAL`, `EVALSHA`, `SCRIPT …`, library functions, `FCALL`, etc.) is **optional**. Build and run with the **`lua` Cargo feature**:

```bash
cargo build --release --features lua
./target/release/kividb
```

Binaries built **without** `--features lua` respond to scripting-related commands with a documented **`ERR`** (subsystem requires Lua).

When enabled, behaviour aims for **Redis compatibility**: sandboxed Lua 5.4 (`mlua`), `redis.call` / `redis.pcall`, script caching, read-only variants, shebang flags on libraries, RESP2/RESP3 script replies, `SCRIPT KILL`, and related semantics. Deeply nested or cyclic single-element table returns stream `*1` prefixes and terminate with `-ERR reached lua stack limit`, matching Redis test expectations.

| Command | Syntax | Description |
|---------|--------|-------------|
| `EVAL` | `EVAL script numkeys key [key ...] arg [arg ...]` | Execute a Lua script |
| `EVALSHA` | `EVALSHA sha1 numkeys key [key ...] arg [arg ...]` | Execute a script already in the cache by SHA1 |
| `EVAL_RO` | `EVAL_RO script numkeys key [key ...] arg [arg ...]` | Read-only script; mutating commands fail |
| `EVALSHA_RO` | `EVALSHA_RO sha1 numkeys key [key ...] arg [arg ...]` | Read-only `EVALSHA` |
| `SCRIPT EXISTS` | `SCRIPT EXISTS sha1 [sha1 ...]` | Test whether SHA1 digests are loaded |
| `SCRIPT FLUSH` | `SCRIPT FLUSH [ASYNC \| SYNC]` | Clear the script cache |
| `SCRIPT LOAD` | `SCRIPT LOAD script` | Load script body; returns SHA1 |
| `SCRIPT KILL` | `SCRIPT KILL` | Stop the running script, if it has not written; one that has is answered `UNKILLABLE`, as in Redis, and runs on (`SHUTDOWN NOSAVE` is the way out) |
| `SCRIPT DEBUG` | `SCRIPT DEBUG YES \| SYNC \| NO` | Accepted; there is no Lua debugger, and scripts run as usual |
| `FUNCTION LOAD` | `FUNCTION LOAD [REPLACE] library-code` | Load a function library (first line `#!lua name=<library>`). The code runs with only `redis.register_function`, `redis.log` and the version constants in reach — no `redis.call`, no `math` — and cannot set globals; it is stopped after 500 ms. A load is all of a library or none of it: an error leaves the library it was to replace as it was. A function's name belongs to one library. Library names are case-sensitive. Refused at the memory limit |
| `FUNCTION DELETE` | `FUNCTION DELETE library-name` | Delete a library and its functions |
| `FUNCTION FLUSH` | `FUNCTION FLUSH [ASYNC \| SYNC]` | Delete every library |
| `FUNCTION LIST` | `FUNCTION LIST [LIBRARYNAME pattern] [WITHCODE]` | Libraries with their functions, each with its description and flags |
| `FUNCTION STATS` | `FUNCTION STATS` | The running function, if any (name, command, duration), and the number of libraries and functions |
| `FUNCTION KILL` | `FUNCTION KILL` | Stop the running function, if it has not written; refused while a script started with `EVAL` is running (`SCRIPT KILL` stops that) |
| `FUNCTION DUMP` / `FUNCTION RESTORE` | `FUNCTION DUMP` / `FUNCTION RESTORE payload [FLUSH \| APPEND \| REPLACE]` | Serialise every library, and load such a payload. The format is KiviDB's own |
| `FCALL` | `FCALL function_name numkeys key [key ...] arg [arg ...]` | Call a registered library function. It is held to the flags it was registered with: with `no-writes` it cannot write; at the memory limit it runs only if it has `allow-oom` or `no-writes` |
| `FCALL_RO` | `FCALL_RO function_name numkeys key [key ...] arg [arg ...]` | Read-only `FCALL`: refused for a function not registered with the `no-writes` flag |

---

## Observability commands

### SLOWLOG

Tracks commands that exceed a configurable execution-time threshold. Stored in a fixed-size ring buffer — oldest entries are evicted when `slowlog-max-len` is reached. The slowlog is per-server (shared across all connections) and persisted only in memory (cleared on restart).

**Configuration** (set at startup via CLI flags):

| Flag | Default | Description |
|------|---------|-------------|
| `--slowlog-log-slower-than` | off (`-1`) | Threshold in microseconds. `0` logs every command, a negative value none. Commands faster than this are not recorded |
| `--slowlog-max-len` | `128` | Maximum number of entries to keep. Oldest entries are evicted when full |
| `--slowlog-entry-max-argc` | `32` | Arguments kept per entry (Redis 8.8); the rest are summarised as `... (N more arguments)` |
| `--slowlog-entry-max-string-len` | `128` | Bytes kept per argument (Redis 8.8); the rest are summarised as `... (N more bytes)` |

**Commands:**

| Command | Syntax | Description |
|---------|--------|-------------|
| `SLOWLOG GET` | `SLOWLOG GET [count]` | Return the last `count` entries (default 10, as in Redis; `slowlog-max-len`, 128, is how many are kept), newest first. Returns an empty array if no entries exist |
| `SLOWLOG LEN` | `SLOWLOG LEN` | Return the current number of entries in the log |
| `SLOWLOG RESET` | `SLOWLOG RESET` | Clear all entries. Returns `OK` |

**Entry format** — each entry is a 7-element array, as in Redis 8.10:

```
*7
:<id>                     ← monotonically increasing integer, unique across the server lifetime
:<unix-timestamp-seconds> ← when the command started (not when it was logged)
:<duration-microseconds>  ← wall-clock time of execute_command only; excludes network I/O
*<n>                      ← the command tokens as a nested array, truncated as configured above
  $<len> <arg>
  ...
$<len> <client-addr>      ← host:port of the client connection
$<len> <client-name>      ← CLIENT SETNAME value, or empty string if unnamed
:<argc>                   ← how many arguments the command had before truncation
```

**Design notes:**

- Timing wraps only `execute_command` — RESP parsing and network I/O are excluded, matching Redis semantics
- Both the fast path (GET/SET/INCR etc.) and the slow path (AUTH, MULTI/EXEC, SUBSCRIBE) are timed
- Commands queued inside a `MULTI` block are each timed individually when `EXEC` runs them
- The underlying `SlowLog` type uses a `parking_lot::RwLock<VecDeque<SlowLogEntry>>` and an `AtomicU64` ID counter — reads never block writes for long
- `SLOWLOG GET`, `SLOWLOG LEN`, and `SLOWLOG RESET` themselves are timed and may appear in the log when `slowlog-log-slower-than = 0`

---

### LATENCY

Tracks per-event latency samples in memory, as Redis's latency monitor does. With `latency-monitor-threshold` set (milliseconds; `0`, the default, is off), a command that takes at least that long is recorded as `fast-command` if Redis flags it fast (`GET`, `SET`, ...) and as `command` otherwise, and an expire cycle as `expire-cycle`. Up to 180 samples per event are retained (matching Redis), oldest evicted first.

| Command | Syntax | Description |
|---------|--------|-------------|
| `LATENCY LATEST` | `LATENCY LATEST` | Return the most recent sample for each tracked event as `[[event, timestamp_secs, latest_ms, max_ms], ...]`, as Redis does |
| `LATENCY HISTORY` | `LATENCY HISTORY event` | Return all stored samples for `event` in chronological order as `[[timestamp_secs, latency_ms], ...]`. Returns an empty array for unknown events |
| `LATENCY RESET` | `LATENCY RESET [event ...]` | Clear all events (no args) or the named ones. Returns the number of events that had samples to clear |
| `LATENCY GRAPH` | `LATENCY GRAPH event` | Return Redis's ASCII graph of the history of `event`: a header with the high and the low, a rule, and a sparkline of `_`, `o`, `#` and `|`. Returns an error if the event has no samples |
| `LATENCY HISTOGRAM` | `LATENCY HISTOGRAM [command ...]` | Per command, its number of calls and how many took less than each power of two of microseconds (`calls`, `histogram_usec`) — a map under RESP3. With no arguments, every command that has been called |
| `LATENCY DOCTOR` | `LATENCY DOCTOR` | Return a human-readable diagnostic report. Flags events whose latest sample exceeds warning (≥100ms) or critical (≥500ms) thresholds. Returns Redis's exact clean-bill-of-health message when no issues are detected |

**Design notes:**

- `command` and `fast-command` are recorded where `execute_command` times the command, by `latency-monitor-threshold`, not by `slowlog-log-slower-than`; `INFO latencystats` gives each command's percentiles (`latency-tracking-info-percentiles`) from the same histogram as `LATENCY HISTOGRAM`
- One lock guards all events: a sample is recorded only for a command over the threshold, never on the fast path
- `SLOWLOG` and `LATENCY` are independent subsystems — resetting one does not affect the other

### HOTKEYS

Find the keys that cost the most (Redis 8.6). While a tracking session runs,
every command's execution time and network bytes are charged to the keys it
names; with no session running the cost to a command is one atomic load.

| Command | Syntax | Description |
|---------|--------|-------------|
| `HOTKEYS START` | `HOTKEYS START METRICS count [CPU] [NET] [COUNT k] [DURATION seconds] [SAMPLE ratio] [SLOTS count slot [slot ...]]` | Start a session tracking the named metrics; `COUNT` is how many keys to report per metric, `DURATION` stops it automatically, `SAMPLE` tracks one command in `ratio`, `SLOTS` restricts it to those hash slots |
| `HOTKEYS STOP` | `HOTKEYS STOP` | Stop the session; its results stay available |
| `HOTKEYS GET` | `HOTKEYS GET` | The session's settings and totals, and the top keys `by-cpu-time-us` and `by-net-bytes`; nil if there is none |
| `HOTKEYS RESET` | `HOTKEYS RESET` | Discard a stopped session's results |

### MEMORY

- **`MEMORY USAGE` estimates are approximate.** Usage is computed by sampling up to `SAMPLES` elements (default 5) and extrapolating — it does not walk every element for large collections. Results will differ from Redis's estimates because Redis tracks allocator-level overhead per object (jemalloc metadata, sds header, dictEntry, etc.) which KiviDB does not model. Treat the output as an order-of-magnitude guide, not an exact byte count.
- **`MEMORY USAGE` is not supported for `TimeSeries` and `VectorSet` keys via `DUMP`/`RESTORE`.** Usage estimation for these types is implemented but the underlying values cannot be serialised portably, so tools relying on DUMP+MEMORY USAGE combinations will not work for these types.
- **`MEMORY STATS`: `used_memory` is what the process has allocated**, counted by the allocator on every platform. The resident-set figures come from `/proc/self/status` on Linux and the process counters on Windows. The allocator's own fragmentation fields are always `1.0`/`0`: KiviDB does not read them from the allocator (the system allocator, or mimalloc on Linux in the builds with Lua).
- **`MEMORY PURGE` calls `malloc_trim(0)` on Linux only.** On other platforms it is a no-op that returns `OK`. Even on Linux, `malloc_trim` only works with glibc's allocator, so it has no effect on what the builds with Lua allocate through mimalloc.
- **`MEMORY MALLOC-STATS` returns a minimal stub XML.** Full `malloc_info` output is not implemented. Tools that parse the detailed allocator statistics from Redis will receive an empty placeholder.
- **`MEMORY USAGE` does not account for expiry metadata.** Keys with a TTL have an additional entry in the expiry store (a separate shard map) that is not included in the reported byte count.

## Not yet supported

| Command / Feature | Notes |
|-------------------|-------|
| `DUMP` / `RESTORE` for TimeSeries and VectorSet | Format not portable; returns ERR by design |
| `XIDMPRECORD` | Redis 8.8 internal command for setting IDMP metadata on an existing stream message. Documented as internal, with no client-facing contract to implement against — omitted deliberately rather than guessed at. Every other Redis 8.8 command is supported; see [`docs/REDIS_8_8_COMMANDS.md`](docs/REDIS_8_8_COMMANDS.md). |

| Compact hash encoding (Redis 8.10) | `HIMPORT` is implemented and creates ordinary hashes; there is no separate memory-saving encoding for hashes that share field names |
| Stemming in Armenian, Basque, Catalan, Chinese, Hindi, Indonesian, Irish, Lithuanian, Malay, Nepali, Serbian, Tagalog and Yiddish; phonetic matching | These languages are accepted and their words are not stemmed (the other 18 Redis languages are stemmed); `PHONETIC` is accepted in a schema and changes nothing. `LANGUAGE_FIELD` is read from hashes, not JSON documents |
| `GEO` and `GEOSHAPE` fields, `GEOFILTER`, `INFIELDS`, `EXPLAINSCORE` | Refused with an error — ignoring them would return the wrong documents. `SUMMARIZE` is accepted and returns fields whole |
| `search-global-timeout` (Redis 8.10) | Not implemented. Query `TIMEOUT`, `search-timeout` and `search-on-timeout` cut a query short; `search-timeout` is `0`, no limit, by default (Redis: 500) |
| `FT.HYBRID` shard ratio and `EXPLAINSCORE` (Redis 8.10) | Not implemented |
| Per-client and server parse-batch metrics in `CLIENT LIST` / `INFO` (Redis 8.8) | Not reported |
| `client-output-buffer-limit` | Not configurable. Pub/Sub subscribers are held to Redis's default (32 MB, or 8 MB for 60 s); replicas to `repl-replica-max-backlog-bytes` |
| A master's replication ID across a restart, without the AOF | A master restarted without the AOF has a new ID, so its replicas resync in full. With the AOF on, a master stopped cleanly keeps its ID and offset, and one restarted after a crash continues a replica that holds the same data. (A restarted *replica* continues by partial resync.) |

The full account of Redis 8.4 – 8.10 is in
[`docs/REDIS_8_4_TO_8_10_PARITY.md`](docs/REDIS_8_4_TO_8_10_PARITY.md).

## Not supported by design

These commands return a documented `ERR` response. They are not stubs —
the error message explains the design decision. Clients that depend on
them will fail loudly rather than silently receiving incorrect results.

Scripting (`EVAL`, `SCRIPT`, `FUNCTION`, `FCALL`, …) is **supported when built with `--features lua`** (see [Scripting commands (Lua)](#scripting-commands-lua)). Default release binaries without that feature return **`ERR`** for those commands.

| Command | Reason |
|---------|--------|
| `MODULE LOAD/LOADEX/UNLOAD` | Requires `dlopen`, ABI versioning, and runtime command-table mutation. The Redis Modules C ABI is incompatible with KiviDB's safety model. `MODULE LIST` names the built-in families clients check for (`search`, `ReJSON`, `timeseries`, `vectorset`) with Redis 8.10's versions and an empty path, as Redis lists its built-in `vectorset`. |
| `FAILOVER` | Only meaningful in a Sentinel topology, which KiviDB does not implement. |
| `CLUSTER SLAVES` | Deprecated by Redis in favour of `CLUSTER REPLICAS`. Use `CLUSTER REPLICAS` instead. |

---

## Functional but with known limitations

| Feature | Limitation |
|---------|-----------|
| `OBJECT REFCOUNT` | Always returns 1; does not reflect actual Arc strong_count |
| `CONFIG SET` | All documented parameters apply immediately at runtime. `loglevel` updates the global level but does not suppress existing log call sites — log filtering is best-effort |
| `OBJECT IDLETIME` under `noeviction` | `0`: when and how often keys are used is recorded only while a `maxmemory-policy` that goes by it is selected, so that the default costs a command nothing |
| RDB import from `dump.rdb` | Every core Redis type, streams and hash fields with their own TTLs included, LZF-compressed strings, RedisJSON documents and function libraries. Search index definitions and dictionaries are left out (run `FT.CREATE` again); a file with other module data (TimeSeries, VectorSet, Bloom) is refused. HyperLogLogs are not in Redis's layout yet ([#62](https://github.com/kividbio/kividb/issues/62)). See [docs/MIGRATING_FROM_REDIS.md](docs/MIGRATING_FROM_REDIS.md) |
| `WAIT` | Blocks using async polling with 1ms sleep intervals via `tokio::time::sleep`; does not use kernel wakeup. Accuracy is sufficient for production but not sub-millisecond |
| `WAITAOF` | Local AOF fsync is triggered immediately on `WAITAOF` via a flush request channel; fsync granularity depends on how fast the AOF writer task is scheduled. Replica AOF ack tracking is not implemented — `replica_fsynced` always returns 0 |
| `maxmemory` enforcement | Measured against allocated memory, and checked on every write (the figure a thread compares with is refreshed every 32 commands). Every Redis policy is implemented, including the 8.6 `allkeys-lrm` / `volatile-lrm`. A replica does not enforce it on what its master sends, and `EXEC` and scripts are checked once, when they start |
| `CLIENT PAUSE` | Enforced at the async task level using tokio cooperative scheduling with 1ms polling. `WRITE`-only mode (reads pass through, writes block) is fully supported |