# Logging

KiviDB writes one line per event, with a timestamp, the process id, the
level and the message:

```text
2026-10-09T07:21:30.114Z 20456 [INFO] KiviDB 1.1.0 starting: pid 20456, default build, config file '/etc/kividb/kividb.conf'
2026-10-09T07:21:30.120Z 20456 [INFO] Ready to accept connections on 0.0.0.0:6379
2026-10-09T07:21:31.516Z 20456 [WARN] maxmemory (1073741824 bytes) reached and nothing more can be evicted under maxmemory-policy noeviction: write commands are refused with OOM. Raise maxmemory, use an evicting policy such as allkeys-lru, or delete data
```

Timestamps are UTC, in ISO 8601 with milliseconds. A warning, error or
fatal line says what happened, what it means for your data, and what to do
about it. [ERRORS.md](ERRORS.md) lists each of them.

The startup banner (the logo, version, licence line and settings summary) and
the update notice above it go to standard output. Log lines go to standard
error, or to `logfile`.

## Settings

| Setting | Values | Default | Changeable at runtime |
|---|---|---|---|
| `loglevel` | `debug`, `verbose`, `notice`, `warning`, `nothing` | `notice` | Yes: `CONFIG SET loglevel verbose` |
| `logfile` | A path, or empty for standard error | Empty | No |
| `log-format` | `text` or `json` | `text` | No |

Each can be set in the config file (`loglevel verbose`) or on the command
line (`--loglevel verbose`). `CONFIG GET` reports all three.

`info`, `warn` and `error` are accepted as older names for `notice`,
`warning` and `nothing`.

### Levels

Each level includes everything above it.

| Level | Tag | What it adds |
|---|---|---|
| `nothing` | `[FATAL]`, `[ERROR]` | Only failures: the server is stopping, or data is not being saved, replicated or loaded as it should be. Unlike Redis's `nothing`, it still logs these, so a server never fails without saying why. |
| `warning` | `[WARN]` | Problems the server works around but you should know about: memory limit reached, a slow script blocking clients, a replica that dropped, a truncated AOF tail, cluster nodes failing. |
| `notice` | `[INFO]` | The server's life story: startup and the settings in force, loading and saving data, replication and cluster role changes, `CONFIG SET` changes (secrets are hidden), shutdown. A few lines a day on a quiet server. **The default.** |
| `verbose` | `[VERBOSE]` | Per-client events: every connection and disconnection (with its address, name and how long it lasted), failed authentication, protocol errors, TLS handshake failures, cluster heartbeats and votes, index backfill progress. Turn it on while chasing a client problem; on a busy server it is a lot of lines. |
| `debug` | `[DEBUG]` | Internals, for debugging KiviDB itself, such as why the update check failed. |

`[FATAL]` lines are always written, whatever the level. They are followed
by the process exiting with status 1.

Errors that can repeat as fast as the work that triggers them (a full disk
fails every AOF write; a file-descriptor limit fails every `accept`) are
logged at most once every 10 seconds per message. Memory-limit warnings are
logged at most once a minute.

Below the configured level, a log call costs one atomic load: the message is
never formatted. `verbose` and `debug` cost nothing unless they are on.

### JSON

`log-format json` writes one JSON object per line, for log pipelines such as
Loki, Elasticsearch, Datadog or CloudWatch:

```json
{"ts":"2026-10-09T07:18:00.443Z","pid":10204,"level":"info","msg":"KiviDB 1.1.0 starting: pid 10204, default build, config file none"}
```

`level` is one of `fatal`, `error`, `warn`, `info`, `verbose` or `debug`.

### Log file

`logfile /var/log/kividb/kividb.log` appends to that file. The server
refuses to start if it cannot open it. For log rotation, use `copytruncate`
in logrotate: the server keeps the file open and does not reopen it.

In containers and under systemd, leave `logfile` unset: standard error is
collected by `docker logs`, `kubectl logs` and `journalctl`.

## What a healthy start looks like

```text
[INFO] KiviDB 1.1.0 starting: pid 4242, full build, config file '/etc/kividb/kividb.conf'
[INFO] I/O model      : io_uring
[INFO] Shards per DB  : 128
[INFO] Found dump.kdb — loading native snapshot
[INFO] Loaded 1204331 keys from snapshot (88213 with a TTL)
[INFO] Ready to accept connections on 0.0.0.0:6379
```

`Ready to accept connections` is the line to wait for in scripts and health
checks. Before it, clients cannot connect. While search indexes are rebuilt
in the background after it, most commands reply `LOADING`.

A clean stop:

```text
[INFO] SIGTERM received — shutting down gracefully
[INFO] Writing final snapshot...
[INFO] Goodbye.
```

With `--io-model tokio`, the middle line reads
`Stopping — writing final snapshot...`.

## Recipes

**Why do clients disconnect?** Turn on `verbose` for a few minutes:

```text
redis-cli CONFIG SET loglevel verbose
# ...reproduce...
redis-cli CONFIG SET loglevel notice
```

Then look for `Client id=… disconnected after …`, `protocol error`,
`Authentication failed` and `TLS: handshake failed`. A client killed for
falling behind on pub/sub is a `[WARN]` line at the default level.

**Is anyone guessing passwords?** Authentication failures are logged at
`verbose`. `ACL LOG` keeps the last ones with their client addresses,
whatever the log level.

**Did a setting change?** Every successful `CONFIG SET` is logged at
`notice` with its new value. The values of `requirepass`, `masterauth` and
`tls-key-file-pass` are shown as `(hidden)`.

**Alerting.** Alert on any `[FATAL]` or `[ERROR]` line. Look at `[WARN]`
lines in daily review, or alert on rate. Each entry in
[ERRORS.md](ERRORS.md) says how urgent it is.
