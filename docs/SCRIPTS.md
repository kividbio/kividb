# Scripts: how they are run, and `lua-parallel-scripts`

Scripts (`EVAL`, `EVALSHA`, `FCALL` and their `_RO` forms) need a build with
Lua: the `lua` and `full` variants.

## What a script may do

- **A script runs in a sandbox.** As in Redis, it has no `package`,
  `require`, `loadfile`, `dofile`, `loadstring`, `print`, `debug` or
  `string.dump`, and `load` takes source text only, not bytecode. (Before
  1.1.0, `package`, `require`, `loadfile`, `dofile`, `debug` and
  `string.dump` were reachable, and with them the host.)
- **A command a script runs is checked against the user's ACL**: `redis.call`
  of a command or key the user may not use fails, as it would if the user
  had sent it.
- **A script that runs longer than `lua-time-limit`** (default 5000 ms; also
  `busy-reply-threshold`) is not stopped, but every other client is told
  `BUSY` until it ends. `SCRIPT KILL` (or `FUNCTION KILL`) stops it if it
  has not written anything; otherwise only `SHUTDOWN NOSAVE` does.

## Lua 5.4, and how it differs from Redis's Lua 5.1

KiviDB runs scripts and function libraries on **Lua 5.4** (`_VERSION` is
`Lua 5.4`). Redis runs them on Lua 5.1. Most scripts written for Redis run
unchanged, BullMQ's and Sidekiq's among them, and KiviDB makes the places
where numbers leave a script behave as Redis's do:

- **`string.format` takes numbers with a fraction for `%d %i %o %u %x %X
  %c`** and cuts them toward zero, as Lua 5.1 does:
  `string.format('%d%03d', t[1], t[2] / 1000)` gives `1731571200123`, and
  `string.format('%d', 1.5)` gives `1`. Lua 5.4 on its own fails with
  "number has no integer representation". A number out of the integer range
  gives what Redis gives (`string.format('%d', 2^63)` and `1e20` are
  `-9223372036854775808`, as Redis gives on x86-64). (Since 1.3.0.)
- **A number passed to `redis.call`** is written as Redis writes it: `10 / 2`
  as `5`, `1e20` as `1e+20`, `2^63` as `9223372036854776000`.
- **A number returned from a script** is cut toward zero to an integer
  reply; one out of the integer range (`1e30`, `math.huge`) is
  `-9223372036854775808`, as Redis gives on x86-64.
- **A vararg function has the implicit `arg` of Lua 5.1** (Redis builds its
  Lua with `LUA_COMPAT_VARARG`): `function f(...) return #arg end` gives
  the number of extra arguments, and `arg.n` is the count, nils included.
  As in Lua 5.1, the function has `arg` only if its text says `arg`, it is
  `nil` in a function whose body uses `...` itself, and in a function that is
  not vararg (or the script's own body) there is no `arg`: the global is
  refused. KiviDB adds it when the script is compiled, to the start of the
  function, on the same line, so error line numbers do not move; a script
  that does not say `arg` in a vararg function is compiled as it was written,
  and nothing is added to what a call does. (Since 1.3.0.)
- **`cjson.encode`** writes floats with 14 significant digits, and `10 / 2`
  as `5`; **`cmsgpack.pack`** writes a number with an integer value as an
  integer, and a float that a 32-bit float holds exactly as one.

- **The Lua 5.1 library functions are there (since 1.3.0):** `table.getn`,
  `table.setn` (which raises `'setn' is obsolete`, as it does in Redis),
  `table.maxn`, `table.foreach`, `table.foreachi`, `math.mod` (C's `fmod`),
  `string.gfind` (`string.gmatch`) and `gcinfo()`. They are in `EVAL`,
  `EVALSHA`, `EVAL_RO` and function libraries. Scripts written for Redis
  2.x and 3.x use them, as hibiken/asynq's do (`table.getn(ids)`). A result
  with an integer value is an integer, as in Redis (`'n=' .. table.maxn(t)`
  is `n=3`). Where they differ from Redis: the error of a bad argument names
  the function (called through `pcall`, Redis names it `'?'`),
  `string.gfind` treats empty matches as Lua 5.4's `string.gmatch` does
  (`'%a*'` over `'abc'` matches once here, twice on Redis), and `gcinfo()`
  counts the memory of the Lua 5.4 VM. They are made the first time a script
  names one (`type(table.getn)` is `function` at once), so `pairs(table)`,
  `pairs(math)` and `pairs(string)` do not list them before that.

What is still Lua 5.4, with the way to write it so that it works on both:

| On KiviDB (Lua 5.4) | On Redis (Lua 5.1) | Works on both |
|---|---|---|
| `/` always gives a float, and a float with an integer value is written with `.0`: `tostring(10 / 2)` is `"5.0"`, so is `'k:' .. 10 / 2`, and a key built that way is a different key | `"5"` | `math.floor(10 / 2)`, or `string.format('%d', n / 2)`, before turning the number into a string |
| Numbers are integers or floats (`math.type`); integers are 64-bit and exact past 2^53: `return 9007199254740993` gives `9007199254740993` | doubles only: `9007199254740992` | keep integers within 2^53 |
| `string.rep`, `string.sub`, `string.char` and other functions that take a position or a count fail on a number with a fraction | cut it toward zero | `math.floor(n)` |
| `//`, `&`, `\|`, `~`, `<<`, `>>`, `goto` and `<const>` are accepted, and `goto` is a keyword | syntax errors | leave them out of scripts that must also run on Redis |
| `unpack` and `table.unpack` are both there, as are `table.pack`, `math.tointeger`, `string.pack` and `utf8` | `unpack` only | `unpack` |
| `load` takes source text | `load` takes a function | |
| `string.format('%s', 10 / 2)` is `"5.0"` | `"5"` | `string.format('%d', n)` for a number you want written without a fraction |
| `string.format('%q', 1.5)` writes the float in hexadecimal (`0x1.8p+0`) | `"1.5"` | `string.format('%q', tostring(x))`, or `%s` |
| `%05s`, `%#s` and `%+s` (flags other than `-` on `%s`) are errors | taken | leave the flag out |
| `bit` returns unsigned 32-bit numbers (`bit.bnot(0)` is `4294967295`) and `bit.tohex` writes capitals | signed (`-1`), lower case | `bit.tobit(x)` on what you compare, `string.lower(bit.tohex(x))` |

Moving scripts to Lua 5.1 entirely, so that these go too, is tracked
separately.

## What a script is to everybody else

One step. No client sees a script half done: not a key it has written and
not yet finished with, not one command of it without the next. That holds
with or without the setting below.

## With `lua-parallel-scripts no`

```
lua-parallel-scripts no           # in the configuration file
--lua-parallel-scripts false      # on the command line
CONFIG SET lua-parallel-scripts no
```

One script runs at a time for the whole server, and no other command runs
while it does. This is what Redis does, and for the same reason: a script
may touch any key, so nothing can be allowed beside it.

On one core that costs nothing. On many it means that a server doing mostly
scripts — a job queue is nearly all scripts — uses one core however many it
has.

Up to 1.0.5 this was the only way. Switch the setting off if a script that
declares keys of one hash tag also touches a key outside it: see "What to
know" below.

## With `lua-parallel-scripts yes` (the default)

Scripts are sorted by the **hash tag** of their keys: the part of a key
between the first `{` and the next `}`, as in `{orders}:waiting`.

- **A script whose `KEYS` all carry a hash tag is held to those tags.**
  That is one tag (`{orders}:waiting {orders}:active`) or several
  (`{api_key:k1}:window {team:t1}:window`). Every key it touches, declared
  or not, must carry one of them; a key built from a declared one that
  keeps its tag (`KEYS[1] .. ':log'`) does. A `redis.call` on any other key
  fails:

  ```
  ERR Script attempted to access the key 'other', which is outside the hash tag 'orders' of its KEYS (lua-parallel-scripts)
  ERR Script attempted to access the key 'other', which is outside the hash tags 'api_key:k1', 'team:t1' of its KEYS (lua-parallel-scripts)
  ```

  (The first key's tag comes first, the others in byte order.)

  So does a command that looks at keys it does not name (`KEYS`, `SCAN`,
  `DBSIZE`, `RANDOMKEY`, `FLUSHALL`, `FLUSHDB`, `SWAPDB`), with its own
  message:

  ```
  ERR Script attempted to run 'keys', which looks at keys it does not name, while running under the hash tag 'orders' of its KEYS (lua-parallel-scripts)
  ```

  A script that runs beside others (see below) gets the same message from
  the other commands that read or change keys they do not name: `INFO`
  with no section or with `keyspace`, `default`, `all` or `everything`,
  `MEMORY STATS`/`DOCTOR`, the searches of an index (`FT.SEARCH`,
  `FT.AGGREGATE`, `FT.HYBRID`, `FT.CURSOR`, `FT.PROFILE`, `FT.INFO`,
  `FT.TAGVALS`, `FT.SPELLCHECK`), the label queries of time series
  (`TS.MGET`, `TS.MRANGE`, `TS.MREVRANGE`, `TS.QUERYINDEX`,
  `TS.QUERYLABELS`), `CLUSTER COUNTKEYSINSLOT`/`GETKEYSINSLOT`/`SLOT-STATS`,
  `FT.DROPINDEX … DD` and `DEBUG POPULATE`. A script that runs alone may
  call them. (`SORT` by a pattern within the script's own tag is allowed:
  it reads only keys the script holds.)

  This is the rule a Redis Cluster applies to a key in another slot, and
  it applies from a script's first run, inside a transaction or not.

- **A script running beside others may not be the first to leave database
  0.** Until something on the server has selected a database other than 0,
  a `SELECT` of another database from a script that is running beside
  others (see below) fails:

  ```
  ERR Script attempted to SELECT 3, the first database other than 0 selected on this server, while running beside other scripts under the hash tag 'orders' of its KEYS (lua-parallel-scripts)
  ```

  A script's first run is always alone, so this only reaches a script that
  selects another database only sometimes, after it has started running
  beside others. Once any client or script has selected a database other
  than 0, scripts may select any database. To avoid it, send `SELECT N`
  once from a client at start-up, or use `lua-parallel-scripts no`.

- **A script running beside others may not `MIGRATE`.** A `MIGRATE` holds
  back writes to the keys it moves until they have left, and a script that
  runs beside others cannot take that hold. It fails instead:

  ```
  ERR Script attempted to run 'migrate' while running beside other scripts under the hash tag 'orders' of its KEYS (lua-parallel-scripts)
  ```

  A script that runs alone (its first run, or one whose `KEYS` carry no
  hash tag) may `MIGRATE`, as before.

  These three are the only things a script may do without the setting that
  it may not do with it.

- **Such scripts run beside each other.** Two scripts that share no tag
  run at the same time, on different cores. Two that share a tag run one
  after the other. A script of several tags holds all of them for the whole
  of its run, so a script of `{a}` and `{b}` waits for one on `{a}` and for
  one on `{b}`, and runs beside one on `{c}`. A script starts running this
  way once the server has seen it to be quick (under a millisecond in the
  VM); its first run, and any script that turns out slow (over five
  milliseconds), runs alone as before.

- **A command that waits for a script on its tag does not hold up its
  thread.** Its connection waits, and the thread it is served by goes on
  serving its other connections, whatever keys they use; what the client
  pipelined behind the command waits with it, and the replies before it
  are sent first. The command runs as soon as the script lets go of the
  tag, in line with the other commands and scripts of that tag in the
  order they came, so a stream of scripts on a tag cannot keep a command
  out, nor the reverse. A command naming keys of several tags holds the
  lock of none of them while it waits, and waits in line only on the tags
  a script is on: scripts on its other tags go on meanwhile. (Should a
  script have come to one of those once it is its turn, it waits for that
  tag too, holding nothing.) The replica's applier and the AOF's replay wait
  in place, as before; the only script they can meet running beside others
  is a read-only one of a replica's client, which the applier waits for.

- **An `EXEC` waits for the scripts running beside others without holding
  up its thread.** It takes the command gate alone, so it runs after every
  script running beside others, whatever their tags. When it finds one
  running, its connection waits asleep until none is, as above: the
  replies before it are sent, what the client pipelined behind it waits
  behind it, and the thread serves its other connections. Scripts that
  come while it waits wait for it, so a stream of them cannot keep it out
  (a script that sees more than three `EXEC`s start to wait goes ahead,
  and the `EXEC`s wait for it too). The transaction then runs whole, alone,
  as before: its `WATCH`es are checked inside the gate, a `CLIENT PAUSE`
  or slot handoff that came meanwhile holds or refuses it whole, and if
  its client goes while it waits, nothing of it runs.

- **Every other script is as before.** A script with no keys, or with a key
  that has no tag, runs alone and may touch what it likes. So do functions
  (`FCALL`), and scripts inside `MULTI`/`EXEC`.

- **A command from a client waits for a script on its tag.** `GET
  {orders}:waiting` sent while a script on `orders` is running is answered
  when the script has finished; so is a command naming a key of any of the
  tags of a script of several. Keys without a tag are never waited for.
  `MULTI`/`EXEC`, `DBSIZE`, `RANDOMKEY`, `FLUSHALL`, `FLUSHDB` and `SWAPDB`
  wait for every running script, and hold back every other command while
  they run; so do `FT.DROPINDEX … DD` and `DEBUG POPULATE`.

- **A command that reads keys it does not name waits for the scripts
  running beside others, and only for them.** `KEYS`, `SCAN`, `INFO` (with
  no section, or `keyspace`, `default`, `all` or `everything`),
  `MEMORY STATS`/`DOCTOR`, the searches of an index (`FT.SEARCH`,
  `FT.AGGREGATE`, `FT.HYBRID`, `FT.CURSOR`, `FT.PROFILE`, `FT.INFO`,
  `FT.TAGVALS`, `FT.SPELLCHECK`), `SORT` by a `BY` or `GET` pattern, the
  label queries of time series (`TS.MGET`, `TS.MRANGE`, `TS.MREVRANGE`,
  `TS.QUERYINDEX`, `TS.QUERYLABELS`) and `CLUSTER COUNTKEYSINSLOT`/
  `GETKEYSINSLOT`/`SLOT-STATS` wait for
  every script that is running beside others, and keep new ones from
  starting until they are done; they run beside every other command, and
  beside each other, as with `lua-parallel-scripts no`. So a reply of
  `KEYS`, or one page of `SCAN`, finds each script's keys as they were
  before it or as they are after it, never part-way through, as in Redis,
  where a script is one step. (Across the pages of a `SCAN`, a key a script
  made or removed meanwhile may or may not be returned, as in Redis.) A
  `KEYS` over millions of keys holds those scripts back for as long as it
  runs, and nothing else: a script waits for it asleep, as it waits for a
  script on its tag, and the thread it would run on goes on serving its
  other connections. A script that finds walk after walk under way (a loop
  of `SCAN`s, each starting before the last has ended) stops waiting
  asleep after three and holds the walks after it back, so it is not kept
  out. A script waits in place, holding its thread, when a walk started
  first or after three walks have started: a long `FT.AGGREGATE` with no
  timeout can therefore hold that thread for its whole run. A node that
  coordinates a search across a cluster holds these locks for its own part
  of the search, and not while it waits for the other masters.

### Using it

Give everything one script works on the same tag. For a job queue that is
one tag per queue; BullMQ takes it as the queue's prefix:

```js
new Queue("orders", { prefix: "{orders}" })
```

Queues with different tags then run on different cores.

A script that works on keys of several tags — a rate limiter that checks a
key's limit and its team's in one call — runs beside others too, as long
as every key it declares carries a tag:

```
EVALSHA <sha> 4 {api_key:k1}:window {api_key:k1}:requests {team:t1}:window {team:t1}:requests 1700000000 60
```

On a cluster, keys of several tags must still hash to one slot, as for
any command: tags of different slots are refused with `CROSSSLOT` first.

Several tags are locked in one fixed order (the order of the lock table,
as a client's command naming keys of several tags locks them), so two
scripts, or a script and a command, that need the same tags never wait for
each other in a cycle. Each tag is held for the whole script, and a
script's writes reach replicas and the AOF as one group while it holds
them: replicas and a replay of the AOF apply scripts in an order the
master could have run them one after another.

### What to know

- A script that declares tagged keys and also uses a key outside their
  tags — a global counter, a lock under another name — will fail where it
  used to work. Either give that key one of the tags (or declare it), or
  declare a key without a tag so that the script runs alone. Up to 1.2.2 a
  script whose `KEYS` carried two different tags was not held to them; it
  is now.
- `SCRIPT KILL` stops every running script that has not yet written, not
  one of them: with several running there is no saying which was meant.
- Each thread that runs scripts this way keeps a Lua VM of its own. Scripts
  cannot leave anything behind in a VM, so this is not something a script
  can observe.
- Two tags can land on the same lock. They then wait for each other when
  they need not; nothing else follows from it.

### Seeing how scripts run

`INFO stats` counts every script run (`EVAL`, `EVALSHA`, their `_RO`
forms, `FCALL`, `FCALL_RO`) from a client, once, by how it was run, so
that a server running its scripts one at a time shows it without
`top -H`:

| Field | Counts a script that |
|---|---|
| `scripts_run_beside_others` | ran beside other commands, under its tags' locks |
| `scripts_run_alone_parallel_off` | ran alone because `lua-parallel-scripts` is `no` |
| `scripts_run_alone_in_transaction` | ran alone inside `MULTI`/`EXEC` |
| `scripts_run_alone_function` | ran alone because it is a function |
| `scripts_run_alone_no_keys` | ran alone because it has no `KEYS` |
| `scripts_run_alone_no_tag` | ran alone because a key of its `KEYS` has no hash tag |
| `scripts_run_alone_not_quick` | ran alone because it has not (or not yet) been seen to be quick |
| `scripts_handed_to_runner` | (alone, and quick) came while another script was running, and was run by the script-runner thread |
| `scripts_offloaded_to_pool` | (not yet seen to be quick, or seen to be slow) was run on a thread of the blocking pool |

A script that runs alone is counted under the first reason in the table
that applies. The `scripts_run_*` counts add up to the script calls that
reached the point of running: a script refused there (`BUSY`, `NOSCRIPT`,
`OOM`), or held back by a `CLIENT PAUSE` that started as it arrived and
then run, is counted each time it got that far. `scripts_handed_to_runner`
and `scripts_offloaded_to_pool` are a second view of the scripts counted as
run alone (a script in one of them is also in a `scripts_run_alone_*`
field): handed to the runner is a quick script that runs alone and came
while another script was running; offloaded to the pool is one not yet
known to be quick, or demoted as slow.
Scripts applied by a replica from its master, or replayed from the AOF,
are not counted. `CONFIG RESETSTAT` sets them all to 0. A build without
Lua has none of these fields.
