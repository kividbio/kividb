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
- **On a cluster, a script's commands use keys of its own slot**, the slot
  of its first key in `KEYS` (or of the first key it uses, with none),
  that this node serves, as in Redis: a command on keys of another slot
  fails with `ERR Script attempted to access keys that do not hash to the
  same slot`, unless the script has the flag `allow-cross-slot-keys`.
- **While its slot is being moved, a script's command on a key it did not
  declare is routed as a client's command would be.** With the slot
  `MIGRATING` away from this node or `IMPORTING` into it (`CLUSTER
  SETSLOT`), a command that a client would be sent elsewhere for (`-ASK`,
  `-MOVED`) or asked to retry (`-TRYAGAIN`) fails in the script with
  `ERR Script attempted to access a non local key in a cluster node`, as
  in Redis: on the node migrating the slot, a key that has left already,
  or that does not exist; on the node importing it (the script sent with
  `ASKING`), a command over several keys of which some have not arrived
  yet. A write to an undeclared key that a `MIGRATE` is moving at that
  moment fails with `TRYAGAIN a key of the script's command is being
  migrated`. The keys in `KEYS` are not affected: they were routed with
  the script, and the script may read, delete and make them again while
  the slot moves. A script that fails part way has run the commands
  before the one that failed, and their writes stay, as in Redis: declare
  every key a script uses in `KEYS` and it never fails this way.

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
  its client goes while it waits, nothing of it runs. While an `EXEC`
  waits for a running script, tagged scripts on other tags that arrive are
  held too (released once more than three `EXEC`s have started waiting),
  so a very long script delays them for its duration.

- **A script whose `KEYS` all lack a tag, and that keeps to them, runs
  beside others under its keys' locks**: see "Scripts that keep to their
  keys" below.

- **Every other script is as before.** A script with no keys, or with a key
  that has no tag (other than those just named), runs alone and may touch
  what it likes. So do functions (`FCALL`), and scripts inside
  `MULTI`/`EXEC`.

- **Scripts that run alone and come while one is running are run right
  after it, on the same thread.** Once a script has been seen to be quick,
  the thread that runs it also runs the quick scripts that came meanwhile,
  one after another, in a run: the other commands are let in again once
  200 microseconds have passed since they were held back and the script
  then running has finished (a run is as long as its last script makes
  it), or once no script is left. Every other command waits once for a
  run, not once for each script in it, and each script is still alone: no
  command runs beside it or between two of them. The first script of a run
  makes its own checks (its routing, ACL, `BUSY`) before it holds the
  others back, as any script does; the scripts after it in the run make
  theirs with the others held back already, and a script that has to wait
  (for a slot handoff, or for a `MIGRATE` of its key) lets them in while
  it waits; a queued script that has to wait so is left to the
  script-runner thread, not run by a thread that serves other connections.
  A server with many such scripts beside plain commands (a lock's
  compare-and-delete, a rate limiter with no hash tag) answers the plain
  commands about as fast as with no scripts at all. The script-runner
  thread runs the ones left over when no script is running.

- **A command from a client waits for a script on its tag.** `GET
  {orders}:waiting` sent while a script on `orders` is running is answered
  when the script has finished; so is a command naming a key of any of the
  tags of a script of several. A key without a tag is waited for only while
  a script that keeps to its keys holds it (see "Scripts that keep to
  their keys").
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

### Scripts that keep to their keys

A script whose `KEYS` all lack a hash tag runs beside others too, held to
its keys as a tagged script is held to its tags, when it is sent by `EVAL`
or `EVALSHA` (not their `_RO` forms) and its source shows that it keeps to
them. That is read from the script's text before it runs (once per thread
and script), and holds when:

- every `redis.call(...)` and `redis.pcall(...)` names its command with a
  string literal (`"GET"`, `'del'`), of the commands below, and gives each
  argument that command takes as a key written exactly as `KEYS[1]`,
  `KEYS[2]`, ...;
- `redis` is used only for its functions (`redis.call`, `redis.sha1hex`,
  `redis.log`, ...), never as a value or assigned to;
- `KEYS` is used only as `KEYS[<number>]`, where it is read: never assigned,
  passed on (`unpack(KEYS)`), counted (`#KEYS`), iterated or shadowed;
- nothing that reaches the globals or functions another way is named:
  `_G`, `_ENV`, `getfenv`, `setfenv`, `loadstring`, `load`, `dofile`,
  `loadfile`, `rawset`, `rawget`, `setmetatable`, `getmetatable`, `debug`,
  `require`, `module`, `package`.

The commands, with their key the first argument: `GET`, `SET`, `SETNX`,
`SETEX`, `PSETEX`, `GETSET`, `GETDEL`, `GETEX`, `INCR`, `INCRBY`, `DECR`,
`DECRBY`, `INCRBYFLOAT`, `APPEND`, `STRLEN`, `GETRANGE`, `SETRANGE`, `TTL`,
`PTTL`, `EXPIRE`, `PEXPIRE`, `EXPIREAT`, `PEXPIREAT`, `EXPIRETIME`,
`PEXPIRETIME`, `PERSIST`, `TYPE`, `HGET`, `HSET`, `HSETNX`, `HMSET`, `HMGET`,
`HDEL`, `HEXISTS`, `HINCRBY`, `HINCRBYFLOAT`, `HGETALL`, `HKEYS`, `HVALS`,
`HLEN`, `HSTRLEN`, `LPUSH`, `RPUSH`, `LPUSHX`, `RPUSHX`, `LPOP`, `RPOP`,
`LLEN`, `LRANGE`, `LINDEX`, `LSET`, `LREM`, `LTRIM`, `LINSERT`, `LPOS`,
`SADD`, `SREM`, `SISMEMBER`, `SMISMEMBER`, `SMEMBERS`, `SCARD`, `SPOP`,
`SRANDMEMBER`, `ZADD`, `ZREM`, `ZSCORE`, `ZMSCORE`, `ZINCRBY`, `ZCARD`,
`ZCOUNT`, `ZRANGE`, `ZRANGEBYSCORE`, `ZREVRANGE`, `ZREVRANGEBYSCORE`,
`ZRANK`, `ZREVRANK`, `ZREMRANGEBYSCORE`, `ZREMRANGEBYRANK`, `ZPOPMIN`,
`ZPOPMAX`; and with every argument a key: `DEL`, `UNLINK`, `EXISTS`, `MGET`,
`TOUCH`.

A lock's release, as yii2-redis and many other clients send it, is such a
script:

```
EVAL "if redis.call(\"GET\",KEYS[1])==ARGV[1] then return redis.call(\"DEL\",KEYS[1]) else return 0 end" 1 <lock> <token>
```

A script that does anything else runs alone, exactly as before, and may
touch what it likes: one that builds a key (`KEYS[1] .. ':log'`), keeps one
in a local (`local key = KEYS[1]`), loops over `KEYS`, or names its command
with a variable. BullMQ's and Sidekiq's scripts are of that kind. Nothing a
script may do changes: the text is read only to decide where the script
runs, before it starts, never part-way through.

Such a script's keys are locked as tags are: each key stands for its own
tag. A command from a client on one of them waits for the script, as a
command on a script's tag does, and a script of the same key waits for it.
So that a command on a key without a tag finds it locked when it needs to,
commands lock keys without a tag too from the first run of such a script on:
that run is alone, and it switches the locks on with every other command
held back, so no command is between its look at whether to lock its keys
and its end. From then on a command on a key without a tag takes that key's
lock, as a command on a tagged key takes its tag's; until then it looks at
the key for a `{` and makes one load more (14 to 17 instructions a command
in all, `GET` and `SET` of 100 B and 1 KB, measured with
`LTO=fat EXEC=1 CMP=plain FEATURES=lua,tls scripts/perf/run.sh`). With the
locks on, plain `GET`/`SET` from memtier (50 connections one command at a
time, and 48 with pipelines of 16) ran at the same rate as with them off,
within the 2 % the runs spread by; a command naming several keys takes a
lock for each: `MGET` of 10 random keys (redis-benchmark, 48 connections,
pipelines of 16) ran at 777,000 a second against 955,000 with them off, of
100 keys at the same rate.

A client's command holds these locks shared, on a tagged key too: commands
wait for a script on their keys, not for each other. Switching `lua-parallel-scripts` off
switches those locks off too, until the next such script.

While keys can be evicted (`maxmemory` set, with a policy other than
`noeviction`), such scripts run alone, as other scripts with a key without a
tag do: making room evicts keys from inside a write, a script's included.

Measured on an Azure `Standard_D8s_v5` (Intel Xeon Platinum 8370C; the
server pinned to 4 vCPUs with `taskset`, `--threads 4`; Redis 8.12 and
Valkey 8.1 with `--io-threads 4`; the load on the other 4): yii2-redis's
`Mutex` cycle, `SET <lock> <token> NX PX 30000` and then the release above
by `EVAL` with the script's text every time, 32 connections each on a lock
of its own, one command at a time per connection, 10 s, three runs each,
the servers in turn (median):

| | ops/s | `EVAL` p50 / p99 (µs) | `SET` p50 / p99 (µs) |
|---|---:|---:|---:|
| this build | 286,200 | 106 / 279 | 100 / 269 |
| the build before | 151,200 | 214 / 458 | 194 / 416 |
| Redis 8.12 | 208,200 | 148 / 293 | 144 / 290 |
| Valkey 8.1 | 232,000 | 130 / 296 | 132 / 297 |

The build before ran each release alone, the other commands held back
around it; its four worker threads were 70 % busy. This build runs them
beside each other, its four threads at 99 %.

### What to know

- A script that declares tagged keys and also uses a key outside their
  tags — a global counter, a lock under another name — will fail where it
  used to work. Either give that key one of the tags (or declare it), or
  declare a key without a tag so that the script runs alone. Up to 1.2.2 a
  script whose `KEYS` carried two different tags was not held to them; it
  is now.
- A key name a script builds while it runs is checked when the script uses
  it, not read from the script's text. One that keeps a tag of the
  script's `KEYS` is allowed (LiteLLM's limiter builds `<prefix>:requests`
  from its window key `{api_key:k1}:window`, and runs beside others). One
  that carries none of them is refused at that call, every time, by
  design: it is the cluster's rule for a key of another slot, and there is
  no setting that lets such a script run beside others. Declare that key
  in `KEYS`, give it one of the tags, or let the script run alone (a key
  without a tag in `KEYS`, or `lua-parallel-scripts no`).
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
| `scripts_run_beside_others` | ran beside other commands, under its tags' locks (or its keys', for a script that keeps to its keys) |
| `scripts_run_alone_parallel_off` | ran alone because `lua-parallel-scripts` is `no` |
| `scripts_run_alone_in_transaction` | ran alone inside `MULTI`/`EXEC` |
| `scripts_run_alone_function` | ran alone because it is a function |
| `scripts_run_alone_no_keys` | ran alone because it has no `KEYS` |
| `scripts_run_alone_no_tag` | ran alone because a key of its `KEYS` has no hash tag, and it is not a script that keeps to its keys (or keys can be evicted; or it is the run that switches on the locks of keys without a tag) |
| `scripts_run_alone_not_quick` | ran alone because it has not (or not yet) been seen to be quick |
| `scripts_handed_to_runner` | (alone, and quick) came while another script was running, and was run by the thread running that one, or by the script-runner thread |
| `scripts_offloaded_to_pool` | (not yet seen to be quick, or seen to be slow) was run on a thread of the blocking pool |
| `scripts_in_alone_runs` | (alone, and quick) took the gate in a run of such scripts, the other commands held back once for the run: counted per take, so a script sent back to wait (a slot handoff, a `MIGRATE` of its key) and run again counts twice |

`scripts_alone_runs` counts the runs: the times the other commands were
held back for one. `scripts_in_alone_runs` over `scripts_alone_runs` is how
many takes a run held, on average; a script alone in its run is a run of
one.

A script that runs alone is counted under the first reason in the table
that applies. The `scripts_run_*` counts add up to the script calls that
reached the point of running: a script refused there (`BUSY`, `NOSCRIPT`,
`OOM`), or held back by a `CLIENT PAUSE` that started as it arrived and
then run, is counted each time it got that far. `scripts_handed_to_runner`
and `scripts_offloaded_to_pool` are a second view of the scripts counted as
run alone (a script in one of them is also in a `scripts_run_alone_*`
field): handed to the runner is a quick script that runs alone and came
while another script was running, run after it by the same thread or by the
script-runner thread; offloaded to the pool is one not yet
known to be quick, or demoted as slow.
Scripts applied by a replica from its master, or replayed from the AOF,
are not counted. `CONFIG RESETSTAT` sets them all to 0. A build without
Lua has none of these fields.
