# Redis Sentinel

KiviDB has no Sentinel of its own, but Redis Sentinel can watch a KiviDB
master and its replicas and fail over between them. Sentinel tells a replica
to take over (`REPLICAOF NO ONE`), points the others at it, and turns the
old master into a replica when it comes back. Clients that find the master
through Sentinel follow it.

This is tested by `scripts/sentinel/run.sh`, which the
[Sentinel workflow](../.github/workflows/sentinel.yml) runs on each release.
Three KiviDB nodes run under three Sentinels from Redis 8.10.3. For each of
redis-py, ioredis and Lettuce, the test:

1. kills the master;
2. checks that a replica is promoted and the other replica follows it;
3. checks that the client goes on writing to the new master without a
   restart;
4. starts the old master again and checks that it comes back as a replica
   with the new master's data.

With `down-after-milliseconds 2000`, a replica is promoted 5 to 9 seconds
after the master dies.

## Setting it up

**Start each node from a config file, named with `--configfile`.** After
every `REPLICAOF` it sends, Sentinel sends `CONFIG REWRITE`, so a node that
restarts keeps the role Sentinel gave it. A node started without a config
file refuses `CONFIG REWRITE`, and the failover still works, but a node
restarted that way comes back with the role on its command line.

In 1.2.2 and earlier, `CONFIG REWRITE` leaves `dir` and the TLS settings out
of the file. A node restarted from the rewritten file then opens the
directory it was started from, and serves without TLS. On those releases,
start each node in its data directory (`WorkingDirectory=` in a systemd
unit), and don't use Sentinel with TLS.

A node, the master and both replicas alike (`replicaof` only on the
replicas):

```
port 6380
dir /var/lib/kividb
requirepass change-me
masterauth change-me
replicaof 10.0.0.1 6380
```

Give every node the same `requirepass` and `masterauth`: any of them may
become the master.

A Sentinel (`redis-sentinel sentinel.conf`, three or more of them, on
separate hosts):

```
port 26379
sentinel monitor kividb 10.0.0.1 6380 2
sentinel auth-pass kividb change-me
sentinel down-after-milliseconds kividb 5000
sentinel failover-timeout kividb 60000
```

Sentinel adds the replicas and the other Sentinels to its file itself, once
it sees them.

**Choosing the replica to promote.** `replica-priority` works as in Redis:
Sentinel promotes the replica with the lowest priority first, and never one
with priority `0`. `replica-announced no` hides a replica from Sentinel.
Both are new in 1.2.1 (see [CONFIGURATION.md](CONFIGURATION.md)).

## Addresses

KiviDB has no `replica-announce-ip`. Sentinel learns each replica's address
from its master's `INFO replication`, which gives the address the replica
connected from. Sentinel and the clients must be able to reach each replica
at that address. On one network, or with host networking, they can. Behind
NAT or Docker's port mapping, they can't, so run the nodes with host
networking, or on addresses everyone can reach, such as pod IPs.
`replica-announced-port` sets the port announced.

## Clients

Point the client at the Sentinels and the master's name, not at a node:

- **redis-py**: `Sentinel([("10.0.0.5", 26379), ...], password=...)`, then
  `master_for("kividb")`, or `slave_for("kividb")` for reads from a replica;
- **ioredis**: `new Redis({ sentinels: [{ host, port }, ...], name: "kividb", password })`;
- **Lettuce**: `RedisURI.Builder.sentinel(host, port, "kividb").withSentinel(...).withPassword(...)`.

These three are the ones tested. A write sent while the master is down
fails, and the client retries it on the new master. Replication is
asynchronous, as in Redis, so a write acknowledged just before the master
died may not have reached the replica that took over. `min-replicas-to-write`
limits how many such writes there can be.

## Running the test

On Linux, with Docker, Python 3, Node and Maven:

```bash
KIVIDB_IMAGE=kividb/kividb:v1.2.2 scripts/sentinel/run.sh
```

Or with binaries, on Linux or macOS:

```bash
KIVIDB_BIN=target/release/kividb SENTINEL_BIN=$(which redis-sentinel) scripts/sentinel/run.sh
```

`CLIENTS="redis-py"` runs one client. `NODE_KIND=redis` runs the same test
with Redis nodes (`KIVIDB_BIN` then names a `redis-server`), to tell a
problem in the test from one in KiviDB.
