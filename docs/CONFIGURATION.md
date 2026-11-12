# Running and configuring KiviDB

- [Running KiviDB](#running-kividb)
- [Protected mode](#protected-mode)
- [Configuration](#configuration)
- [TLS](#tls)
- [Logs and errors](#logs-and-errors)
- [Startup update check](#startup-update-check)

## Running KiviDB

```bash
./target/release/kividb                    # listens on 0.0.0.0:6380
redis-cli -p 6380 PING
```

Any Redis client works; point it at port **6380**. KiviDB listens on **6380**, not Redis's
6379, so the two can run side by side; point your client at that port, or pass
`--port 6379`. The default listen address is `0.0.0.0:6380`, with
[protected mode](#protected-mode) on.

[INSTALL.md](INSTALL.md) covers the packages, images and binaries;
[BUILDING.md](BUILDING.md) covers building from source.

## Protected mode

Protected mode is **on by default**, as in Redis. While it is on, **no bind address has been
set** (`--bind` or `bind` in the config file) and the **`default` user has no password**, only
clients on the same machine (loopback) may connect. Any other client
receives a `-DENIED` error that explains the options, and is disconnected.

To accept clients from other machines, do one of these:

- set a password: `--requirepass <password>`, or `ACL SETUSER default ><password>`;
- set a bind address: `--bind <address>`;
- turn protected mode off: `--protected-mode no`, `protected-mode no` in the config file, or
  `CONFIG SET protected-mode no` from a local client.

If you turn it off or bind to a public address, make sure the server cannot be reached from
the internet without a password.

The Docker image runs with protected mode off by default; see
[Docker](INSTALL.md#docker).

## Configuration

KiviDB is configured with command-line flags, or with a Redis-style configuration file
(one directive per line, such as `port 6380` or `appendonly yes`). The file is read from
`/etc/kividb/kividb.conf` if it exists; `--configfile <path>` names another one. A flag given
on the command line wins over the same setting in the file. Run `kividb --help` for the full
list.

| Flag | Default | Description |
|------|---------|-------------|
| `--port` | `6380` | Port to listen on; `0` for none (TLS only, with `--tls-port`) |
| `--bind` | `0.0.0.0` | Address to listen on. Setting it explicitly turns off [protected mode](#protected-mode) |
| `--protected-mode <yes\|no>` | `yes` | See [Protected mode](#protected-mode) |
| `--threads` | `0` (auto) | Worker threads; auto is the number of cores minus 2 (all cores below 4) |
| `--databases` | `16` | Number of databases |
| `--dir <path>` | start directory | Where `dump.kdb`, `appendonly.aof` and `nodes.conf` live |
| `--aof` | off | Append-only file persistence |
| `--maxmemory <bytes>` | `0` (unlimited) | Memory limit |
| `--maxmemory-policy` | `noeviction` | What happens at the limit: `noeviction`, or the `allkeys-*` / `volatile-*` eviction policies (`kividb --help` lists them) |
| `--loglevel` | `info` | `debug`, `verbose`, `notice` (or `info`), `warning`, `nothing` |
| `--logfile <path>` | standard error | Write the log to a file |
| `--log-format <text\|json>` | `text` | Log line format |
| `--io-model` | `auto` | `auto`, `uring` or `tokio` (io_uring is Linux only) |
| `--requirepass <password>` | none | Password for the `default` user |
| `--aclfile <path>` | none | ACL file to load users from |
| `--replicaof <HOST> <PORT>` | none | Replicate from a primary |
| `--replica-announced-port <port>` | server port (the TLS port with `--tls-replication yes` or `--port 0`) | Port announced to the primary via `REPLCONF listening-port` |
| `--replica-priority <n>` | `100` | Sentinel promotes the replica of lowest priority first, and never one of `0`. New in 1.2.1: 1.2.0 has no such flag, and ignores the setting in the file |
| `--replica-announced <yes\|no>` | `yes` | Whether Sentinel lists this replica. New in 1.2.1, as above |
| `--slowlog-log-slower-than <us>` | off | Slowlog threshold in microseconds (`0` logs every command) |
| `--slowlog-max-len <n>` | `128` | Slowlog entries kept |
| `--cluster-enabled <yes\|no>` | `no` | Cluster mode: a cluster bus on `<port> + 10000`, topology in `nodes.conf`, gossip, failure detection and failover |
| `--cluster-node-timeout <ms>` | `15000` | Time without a reply before a peer is suspected to have failed |
| `--tls-port <port>` | `0` (off) | TLS listener (TLS builds only); with `--port 0`, the only one |
| `--tls-cert-file`, `--tls-key-file` | none | Server certificate and key (PEM) |
| `--tls-ca-cert-file` | none | CA that client certificates are checked against |
| `--tls-auth-clients <yes\|optional\|no>` | `yes` | Whether clients must present a certificate (only with `--tls-ca-cert-file`) |
| `--tls-auth-clients-allowed-names <names>` | none | Names a client certificate's CN or DNS name must match; `*.` is one label |
| `--update-check <yes\|no>` | `yes` | The [startup update check](#startup-update-check) |

`maxclients` (default `10000`, in the file or with `CONFIG SET`) counts plaintext and TLS
clients, TLS ones from the start of their handshake. At startup KiviDB raises the soft
open-file limit to `maxclients` plus what it keeps for itself (32 files, or on io_uring 40 and
8 a worker thread) if the hard limit allows; if not, it lowers `maxclients` to fit and logs a
warning. Cluster bus links, a replica's link to its master and files written while saving
are not counted in that; leave room for them in a large cluster.

Persistence (snapshots and the AOF) is described in [PERSISTENCE.md](PERSISTENCE.md).

## TLS

TLS is an optional Cargo feature (`--features tls`): it is in the `tls` and `full` builds
(see [BUILDING.md](BUILDING.md#builds)).

TLS clients connect on a separate port, `--tls-port`; plain traffic stays on `--port`. With
`--port 0` there is no plaintext port, and the server is reached over TLS only (it needs a
`--tls-port`; a cluster node needs `--tls-cluster yes` as well, and its bus is then
`<tls-port> + 10000`). A replica of such a master connects with `--tls-replication yes`.

```bash
cargo build --release --features tls
./target/release/kividb --port 6380 --tls-port 6381 --tls-cert-file cert.pem --tls-key-file key.pem
redis-cli --tls -p 6381 --cacert ca.pem   # ca.pem: the CA that signed cert.pem
```

**Client certificates (mutual TLS).** With `--tls-ca-cert-file ca.pem`, clients must present a
certificate signed by that CA; others are refused during the handshake.
`--tls-auth-clients optional` accepts clients without a certificate but still refuses an
invalid one, and `--tls-auth-clients no` does not ask for one. With `redis-cli`:
`redis-cli --tls -p 6381 --cacert ca.pem --cert client.pem --key client.key`.

**Pinning client certificates by name.** `--tls-auth-clients-allowed-names "*.db1.example
admin.example"` (spaces or commas between names) accepts a client certificate only if its subject
CN or one of its DNS names matches one of the names; others signed by the same CA fail the
handshake, and the server logs the names they carried. `*` stands for exactly one label:
`*.db1.example` takes `node-0.db1.example`, not `a.b.db1.example` or `db1.example`. Case does
not matter. It applies to clients, replicas and, with `tls-cluster`, nodes on the cluster bus;
empty (the default) is off. KiviDB's own setting: Redis has none.

**Changed without a restart.** `CONFIG SET` of `tls-cert-file`, `tls-key-file`,
`tls-ca-cert-file`, `tls-auth-clients`, `tls-client-cert-file` and `tls-client-key-file` loads the
files again for every handshake from then on. `CONFIG SET tls-port <port>` opens the TLS port
(also on a server that started without TLS: `CONFIG SET tls-cert-file cert.pem tls-key-file
key.pem tls-port 6381`), moves it, or closes it with `0`; connections already open stay open.
`CONFIG SET tls-replication yes` makes a replica's next connection to its master TLS. What cannot
be loaded or listened on is refused, and nothing changes. `tls-cluster` needs a restart, and so
does `tls-port` on a node with `tls-cluster yes`.

**Clients must use TLS on the TLS port.** A plain connection to it (for example `redis-cli -p 6381`
without `--tls`) fails the handshake; at `--loglevel verbose` the server logs
`TLS: handshake failed from <address>`. For
memtier_benchmark, add `--tls` (and `--tls-skip-verify` for a self-signed certificate).

## Logs and errors

The log goes to standard error, or to `--logfile`. [LOGGING.md](LOGGING.md)
describes the log; [ERRORS.md](ERRORS.md) explains the warnings, errors and error
replies, and what to do about each.

## Startup update check

When it starts, a release build of KiviDB makes **one HTTPS request** to
`https://github.com/kividbio/kividb/releases/latest`, the same page you would open to find
the newest release. GitHub answers with a redirect to that release's tag; if it is newer
than the running build, KiviDB prints a short notice with the link before the startup
banner. Nothing is sent but the request itself, with a `User-Agent` of `kividb/<version>`:
no host name, configuration, keys, data or identifier. GitHub sees the request's source IP
address, as any web server would.

Startup waits at most 1.5 seconds for the reply; a failure is logged at debug level only and
never affects clients. Only a version-shaped tag is accepted from the reply, and nothing
else from it is printed.

Any one of these turns it off:

- `--update-check no` on the command line;
- `update-check no` in the configuration file;
- `KIVIDB_NO_UPDATE_CHECK=1` in the environment.

It is also never made by a debug build or when the `CI` environment variable is set.
`--update-check-url` points it at another `https://` address, such as a mirror inside a
closed network. [UPDATE_CHECK.md](UPDATE_CHECK.md) has the details.
