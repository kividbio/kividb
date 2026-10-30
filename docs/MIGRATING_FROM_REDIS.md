# Moving from Redis to KiviDB

KiviDB speaks the Redis protocol (RESP2 and RESP3), so a Redis client
connects to it unchanged. This page covers what does change when you move an
application or a dataset across, in the order you'll meet it. What differs
command by command is in [COMPATIBILITY.md](COMPATIBILITY.md).

## 1. Pick the build

KiviDB comes in four variants. Pick it first, because the default has no Lua:

| You use | Variant | Docker tag | Release file |
|---|---|---|---|
| Plain commands, Pub/Sub, transactions, search, JSON | `default` | `latest`, `v1.1.2` | `kividb-linux-x86_64.tar.gz`, `.deb`, `.rpm` |
| `EVAL`, `FUNCTION`, `FCALL`: BullMQ, Sidekiq, rate limiters and most job queues | `lua` | `v1.1.2-lua` | `kividb-lua-linux-x86_64.tar.gz` |
| TLS to clients, replicas or cluster nodes | `tls` | `v1.1.2-tls` | `kividb-tls-…` |
| Both | `full` | `v1.1.2-full` | `kividb-full-…` |

In a build without Lua, the scripting commands return an error. They don't
quietly do nothing. A build without TLS refuses to start if `--tls-port` is
set. It won't serve plaintext instead.

If you aren't sure whether your application runs scripts, run
`INFO commandstats` on your Redis and look for `cmdstat_eval`,
`cmdstat_evalsha` and `cmdstat_fcall`.

## 2. Port and network

- **KiviDB listens on 6380, not 6379**, so the two can run side by side
  while you move. Pass `--port 6379` (or `port 6379` in the config file) to
  take Redis's place.
- **Protected mode is on by default.** With no password and no explicit
  `--bind`, only loopback clients are accepted, as in Redis. Set
  `--requirepass` or an ACL file before you open it to the network. The
  Docker image starts with `--protected-mode no`, and any arguments you pass
  replace that default (see the README's Docker section).

## 3. Configuration

KiviDB reads a Redis-style config file: one directive per line, from
`/etc/kividb/kividb.conf` or `--configfile <path>`. Most of the directives
you already use carry over, under the same names.

**Don't copy a whole `redis.conf` across.** The file is read strictly: a
directive KiviDB doesn't know stops the server, with an error naming it.
Start from KiviDB's own file and carry over the settings you changed.
Directives for machinery KiviDB doesn't have (`io-threads`, `hz`,
`tcp-keepalive`, `lazyfree-*`, `rdbcompression` and similar) are accepted
with a warning and do nothing.

The ones that behave differently:

| Directive | In KiviDB |
|---|---|
| `io-threads` | Accepted, no effect: commands run on every core. `--threads` sets the worker count (default: cores − 2). |
| `save <points>` | Accepted with a warning, no effect. KiviDB doesn't snapshot on a schedule. Use `appendonly yes`, or `BGSAVE`. |
| `appendfsync` | Only `everysec`. `always` and `no` are accepted with a warning. |
| `dbfilename`, `appendfilename` | Fixed: `dump.kdb` and `appendonly.aof`. KiviDB reads a Redis `dump.rdb` if there is no `dump.kdb`. |
| `client-output-buffer-limit` | Not supported: remove it. Pub/Sub subscribers get Redis's default limits. |
| `loadmodule` | Not supported: remove it. Search (`FT.*`), JSON, time series (`TS.*`) and vector sets are built in. |
| Sentinel | No Sentinel. Use replication with your own failover, or cluster mode (`--cluster-enabled yes`), which fails over by itself. |

Run `kividb --help` for the command-line flags.

## 4. Move the data

There are three ways, from simplest to least downtime.

**Copy the snapshot.** Stop writes to Redis, run `SAVE` or `BGSAVE`, and put
its `dump.rdb` in KiviDB's `--dir`. KiviDB loads it at startup if there is no
`dump.kdb` there. It writes its own `dump.kdb` from then on.

**Load a backup at startup.** `kividb --preload-file rdb:/path/to/dump.rdb`
loads that file *instead of* what's in `--dir`. If the file is missing or
can't be read, the server doesn't start, so you never get an empty server by
mistake.

**Replicate from Redis.** Start KiviDB with `--replicaof <redis-host>
<redis-port>` (and `masterauth` if your Redis needs a password). It takes a
full sync from the Redis primary, then follows its writes. When it has
caught up, point the application at KiviDB and run `REPLICAOF NO ONE` on it.
This is the way to move with only a moment of downtime. Before you switch
over, compare `DBSIZE` on both sides and spot-check a few keys of each type.

What the RDB reader takes from Redis:

- **Every core type:** strings, lists, sets, sorted sets, hashes (including
  hash fields with their own TTLs, Redis 7.4+), and streams (Redis 7.0 to
  8.10 encodings). Key expiry times come across too.
- **RedisJSON documents**, and Lua function libraries (`FUNCTION LOAD`).
- **Search index definitions and dictionaries are not loaded.** KiviDB keeps
  its own indexes. Run `FT.CREATE` (and `FT.DICTADD`) again after the move;
  `FT.CREATE` indexes the keys that are already there.
- **Other module data** (time series, vector sets, Bloom filters, and so on)
  is refused, and so is the whole file: KiviDB won't load part of a
  snapshot. Move those keys with `MIGRATE` instead, or recreate them.
- **HyperLogLogs aren't usable yet.** KiviDB's HyperLogLog registers aren't
  in Redis's layout ([#62](https://github.com/kividbio/kividb/issues/62)), so
  a HyperLogLog copied from Redis gives wrong counts or is refused. Rebuild
  it with `PFADD` from the source data if you can, or leave those keys on
  Redis for now.

## 5. Persistence and durability

- Turn on the append-only file (`--aof` or `appendonly yes`) if you need
  writes to survive a restart. Without it, data is only on disk after a
  `BGSAVE`, or at a clean shutdown.
- **With the AOF on, a write is answered once its record is in the file**,
  as in Redis: a `kill -9` or an OOM kill loses no acknowledged write, and a
  power failure loses up to a second, as with Redis's `appendfsync
  everysec`. Before 1.2 a write was answered once its record was queued,
  and a hard kill could lose the last few; `aof-ack-after-write no` still
  does that, for more writes a second (see [PERSISTENCE.md](PERSISTENCE.md)).
- [PERSISTENCE.md](PERSISTENCE.md) covers snapshots, checkpoints, backups
  and what recovery guarantees.

## 6. Replication and cluster

- Replication uses `REPLICAOF` / `--replicaof`, with partial resync, also
  after a failover: a promoted replica keeps the old master's stream as
  its second replication id (`master_replid2`), so the old master's other
  replicas continue from it rather than resync in full. A
  KiviDB replica of a KiviDB master is the usual setup. A replica of a Redis
  master is the migration path above.
- Cluster mode (`--cluster-enabled yes`) uses Redis's cluster protocol: hash
  slots, `CLUSTER` commands, a cluster bus on `<port> + 10000`, gossip,
  failure detection and failover. Cluster-aware clients work unchanged.
  KiviDB nodes only cluster with KiviDB nodes; a mixed Redis/KiviDB cluster
  isn't supported.
- On Kubernetes, [kividb-operator](https://github.com/kividbio/kividb-operator)
  runs replicated KiviDB with failover and backups.

## 7. Check your application

1. Try a client first: [examples/clients/](../examples/clients/) has tested
   examples for redis-py, ioredis, BullMQ, go-redis and Jedis.
2. Run your test suite against KiviDB. A command KiviDB lacks returns
   `ERR unknown command`; [ERRORS.md](ERRORS.md) explains every error KiviDB
   adds.
3. Check [COMPATIBILITY.md](COMPATIBILITY.md) for the commands you rely on.
4. Measure with your own workload. The published benchmarks are one
   controlled setup, not a promise.

Something on this page wrong or missing for your setup?
[Open an issue](https://github.com/kividbio/kividb/issues/new/choose).
