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

What is still Lua 5.4, with the way to write it so that it works on both:

| On KiviDB (Lua 5.4) | On Redis (Lua 5.1) | Works on both |
|---|---|---|
| `/` always gives a float, and a float with an integer value is written with `.0`: `tostring(10 / 2)` is `"5.0"`, so is `'k:' .. 10 / 2`, and a key built that way is a different key | `"5"` | `math.floor(10 / 2)`, or `string.format('%d', n / 2)`, before turning the number into a string |
| Numbers are integers or floats (`math.type`); integers are 64-bit and exact past 2^53: `return 9007199254740993` gives `9007199254740993` | doubles only: `9007199254740992` | keep integers within 2^53 |
| `string.rep`, `string.sub`, `string.char` and other functions that take a position or a count fail on a number with a fraction | cut it toward zero | `math.floor(n)` |
| `//`, `&`, `\|`, `~`, `<<`, `>>`, `goto` and `<const>` are accepted, and `goto` is a keyword | syntax errors | leave them out of scripts that must also run on Redis |
| `unpack` and `table.unpack` are both there, as are `table.pack`, `math.tointeger`, `string.pack` and `utf8` | `unpack` only | `unpack` |
| `load` takes source text; `gcinfo`, `table.getn`, `table.maxn`, `math.mod` and `string.gfind` are not there | `load` takes a function; the others are there | `#t` for `table.getn(t)`, `math.fmod` for `math.mod`, `collectgarbage('count')` for `gcinfo()` |
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

- **A script whose `KEYS` all carry the same hash tag is held to that tag.**
  Every key it touches, declared or not, must carry it. A `redis.call` on
  any other key fails:

  ```
  ERR Script attempted to access the key 'other', which is outside the hash tag 'orders' of its KEYS (lua-parallel-scripts)
  ```

  So does a command that looks at keys it does not name (`KEYS`, `SCAN`,
  `DBSIZE`, `RANDOMKEY`, `FLUSHALL`, `FLUSHDB`, `SWAPDB`), with its own
  message:

  ```
  ERR Script attempted to run 'keys', which looks at keys it does not name, while running under the hash tag 'orders' of its KEYS (lua-parallel-scripts)
  ```

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

  These two are the only things a script may do without the setting that
  it may not do with it.

- **Such scripts run beside each other.** Two scripts on different tags run
  at the same time, on different cores. Two on the same tag run one after
  the other. A script starts running this way once the server has seen it
  to be quick (under a millisecond in the VM); its first run, and any
  script that turns out slow (over five milliseconds), runs alone as
  before.

- **Every other script is as before.** A script with no keys, with a key
  that has no tag, or with keys of two tags runs alone and may touch what
  it likes. So do functions (`FCALL`).

- **A command from a client waits for a script on its tag.** `GET
  {orders}:waiting` sent while a script on `orders` is running is answered
  when the script has finished. Keys without a tag are never waited for.
  `MULTI`/`EXEC`, and the commands that look at the whole keyspace, wait
  for every running script.

### Using it

Give everything one script works on the same tag. For a job queue that is
one tag per queue; BullMQ takes it as the queue's prefix:

```js
new Queue("orders", { prefix: "{orders}" })
```

Queues with different tags then run on different cores.

### What to know

- A script that declares tagged keys and also uses a key outside the tag —
  a global counter, a lock under another name — will fail where it used to
  work. Either give that key the tag, or declare a key without a tag so
  that the script runs alone.
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
| `scripts_run_beside_others` | ran beside other commands, under its tag's lock |
| `scripts_run_alone_parallel_off` | ran alone because `lua-parallel-scripts` is `no` |
| `scripts_run_alone_in_transaction` | ran alone inside `MULTI`/`EXEC` |
| `scripts_run_alone_function` | ran alone because it is a function |
| `scripts_run_alone_no_keys` | ran alone because it has no `KEYS` |
| `scripts_run_alone_no_tag` | ran alone because a key of its `KEYS` has no hash tag |
| `scripts_run_alone_several_tags` | ran alone because its `KEYS` carry different hash tags |
| `scripts_run_alone_not_quick` | ran alone because it has not (or not yet) been seen to be quick |
| `scripts_handed_to_runner` | (alone, and quick) came while another script was running, and was run by the script-runner thread |
| `scripts_offloaded_to_pool` | (not yet seen to be quick, or seen to be slow) was run on a thread of the blocking pool |

A script that runs alone is counted under the first reason in the table
that applies. The `scripts_run_*` counts add up to the scripts run; the
last two count, of those, where the ones that ran alone were run.
Scripts applied by a replica from its master, or replayed from the AOF,
are not counted. `CONFIG RESETSTAT` sets them all to 0. A build without
Lua has none of these fields.

A benchmark of a script that declares tagged keys and still shows
`scripts_run_alone_several_tags` going up is sending keys of different
tags: `memtier_benchmark` draws a new key for every `__key__` in a
command, so `{__key__}:a {__key__}:b` names two tags unless the command
is run with `--transaction`, or the second key is built from the first in
the script.
