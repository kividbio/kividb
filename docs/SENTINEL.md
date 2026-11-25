# Redis Sentinel

KiviDB has no Sentinel of its own, but Redis Sentinel can watch a KiviDB
master and its replicas and fail over between them. Sentinel tells a replica
to take over (`REPLICAOF NO ONE`), points the others at it, and turns the
old master into a replica when it comes back. Clients that find the master
through Sentinel follow it.

This is tested by `scripts/sentinel/run.sh`, which the
[Sentinel workflow](../.github/workflows/sentinel.yml) runs on each release
and by hand. Three KiviDB nodes run under three Sentinels from Redis 8.10.3,
the nodes and the Sentinels each with a password. The master fails over
three ways:

- **killed** (SIGKILL), once with each of redis-py, ioredis and Lettuce, and
  started again after;
- **by hand**, with `SENTINEL FAILOVER`, the master up throughout;
- **paused** (SIGSTOP) until a replica has been promoted, then let go: it
  comes back as a master, and Sentinel turns it into a replica.

Each time, the test checks five things:

1. a replica is promoted, and the other replica follows it;
2. the client goes on writing to the new master without a restart;
3. the old master comes back as a replica, with the client's writes;
4. the new master's config file names no master;
5. the others' config files name the new master.

With `down-after-milliseconds 2000`, a replica is promoted 4 to 9 seconds
after the master dies or stops.

## Setting it up

**Start each node from a config file, named with `--configfile`.** After
every `REPLICAOF` it sends, Sentinel sends `CONFIG REWRITE`, so a node that
restarts keeps the role Sentinel gave it. A node started without a config
file refuses `CONFIG REWRITE`, and the failover still works, but a node
restarted that way comes back with the role on its command line.

In 1.2.2 and earlier, `CONFIG REWRITE` leaves many settings out of the
file, `dir` and the TLS settings among them. It also keeps `replicaof` after
`REPLICAOF NO ONE`. After a failover, a node restarted from its file then:

- opens the directory it was started from;
- serves without TLS;
- if it was the replica Sentinel promoted, follows its old master again.

On those releases, start each node in its data directory
(`WorkingDirectory=` in a systemd unit), don't use Sentinel with TLS, and
check a promoted node's file before restarting it.

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
requirepass sentinel-password
sentinel sentinel-pass sentinel-password
sentinel monitor kividb 10.0.0.1 6380 2
sentinel auth-pass kividb change-me
sentinel down-after-milliseconds kividb 5000
sentinel failover-timeout kividb 60000
```

Sentinel adds the replicas and the other Sentinels to its file itself, once
it sees them. `requirepass` protects the Sentinel itself, and
`sentinel-pass` is what it uses with the other Sentinels: give them all the
same one. The clients need it too (below).

**Choosing the replica to promote.** `replica-priority` works as in Redis:
Sentinel promotes the replica with the lowest priority first, and never one
with priority `0`. `replica-announced no` hides a replica from Sentinel.
Both are new in 1.2.1 (see [CONFIGURATION.md](CONFIGURATION.md)).

## Addresses

Sentinel learns each replica's address from its master's `INFO
replication`, which gives the address the replica connected from. Sentinel
and the clients must be able to reach each replica at that address.

- **They can** on one network, with host networking, or with Sentinel and
  the clients inside the same Kubernetes cluster (pod IPs).
- **They can't** behind NAT or Docker's port mapping. There, the master
  reports a replica at its container address, with the port
  `replica-announced-port` gives, for example `172.22.0.3:17602`, and no
  one outside reaches that.

KiviDB accepts `replica-announce-ip` but does not act on it, so there is no
setting that fixes this yet. Run the nodes with host networking, or on
addresses everyone can reach.

## Clients

Point the client at the Sentinels and the master's name, not at a node:

- **redis-py**: `Sentinel([("10.0.0.5", 26379), ...], password=..., sentinel_kwargs={"password": ...})`,
  then `master_for("kividb")`, or `slave_for("kividb")` for reads from a
  replica;
- **ioredis**: `new Redis({ sentinels: [{ host, port }, ...], name: "kividb", password, sentinelPassword })`;
- **Lettuce**: `RedisURI.Builder.sentinel(host, port, "kividb", sentinelPassword).withSentinel(host, port, sentinelPassword).withPassword(...)`.

These three are the ones tested. A write sent while the master is down
fails, and the client gets the error. Once the client has found the new
master through the Sentinels, its writes go there. Replication is
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

`CLIENTS="redis-py"` runs the kill round with that client only, and the
other two rounds with the first one listed. `NODE_KIND=redis` runs the same test
with Redis nodes (`KIVIDB_BIN` then names a `redis-server`), to tell a
problem in the test from one in KiviDB.
