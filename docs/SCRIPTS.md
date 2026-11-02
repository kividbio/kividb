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

  This is the one
  thing a script may do without the setting that it may not do with it. It
  is the rule a Redis Cluster applies to a key in another slot, and it
  applies from a script's first run, inside a transaction or not.

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
