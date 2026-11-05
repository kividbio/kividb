# OpenViking

OpenViking can use Redis in four places, all optional: the RAGFS cache runtime (read cache, QueueFS and the
PathLock provider, in Rust), the gateway's storage, the expiry scheduler (session, compile and TTL callbacks),
and the account store (`redis_mysql`). KiviDB works for all the ones tested here. Use the **`-lua` image**
(`kividb/kividb:v1.2.0-lua`): every one of them runs Lua scripts (`EVALSHA`, `SCRIPT LOAD`).

**Checked** with OpenViking `v0.5.3`, on KiviDB 1.2.0 and on Redis 8.10.3, with the same pins. The runner,
pins and raw results are in [`benchmarks/openviking-compat/`](../../benchmarks/openviking-compat/)
([KiviDB](../../benchmarks/openviking-compat/results-kividb.json),
[Redis 8](../../benchmarks/openviking-compat/results-redis8.json)).

| Suite | KiviDB 1.2.0 | Redis 8.10.3 |
|---|---|---|
| RAGFS, `cargo test -p ragfs` with its Redis tests on (standalone, 3-node cluster, password) | 586 passed, 1 failed | 586 passed, 1 failed |
| Gateway, `tests/gateway` as its CI runs it (standalone + 3-node cluster, ACL user) | 1163 passed, 34 failed | 1197 passed |
| The same, with `+command` added to the ACL user | 1197 passed | (not needed) |
| PathLock filesystem-to-Redis migration (`tests/agfs/test_pathlock_provider_migration.py`) | 9 passed | 9 passed |
| Expiry scheduler, our harness (no OpenViking test covers it against a server) | 8 of 8 | 8 of 8 |

The RAGFS failure is the same test on both servers, and it fails without any Redis too
(`core::mountable::tests::shutdown_reaches_cache_queuefs_through_stats_wrapper`), so it says nothing about a
server.

## Known gap: `COMMAND` and `@connection` (#392)

The gateway's CI gives its Redis user `+@connection` and no `+command`. On Redis 8 that category includes
`COMMAND`, which redis-py's cluster client runs when it connects. KiviDB 1.2.0 leaves `COMMAND` out of
`@connection` ([#392](https://github.com/kividbio/kividb/issues/392)), so with that exact user every cluster
connection fails with `NoPermissionError: User gateway has no permissions to run the 'command' command`.
That is all 34 gateway failures. Standalone connections are not affected.

**Workaround:** add `+command` to the user's ACL rules:

```
ACL SETUSER gateway +command
```

With it, all 1197 gateway tests pass on KiviDB.

## Configure OpenViking for KiviDB

```bash
docker run -d -p 6380:6380 kividb/kividb:v1.2.0-lua
```

Point the Redis endpoints at KiviDB (port `6380`), for example the RAGFS cache in `ov.conf`:

```json
{
  "cache": {
    "provider": "redis",
    "params": { "mode": "standalone", "endpoints": ["redis://127.0.0.1:6380"] }
  }
}
```

The expiry scheduler (`expiry.redis.endpoints`) and the gateway (`storage.redis.endpoints`) take the same
form. Cluster mode works too (`"mode": "cluster"` with the nodes' endpoints).

## Not tested

- The account store (`redis_mysql`): it needs MySQL as well, and has no test against a real server.
- Redis Sentinel (`REDIS_SENTINEL_TEST_URLS`), the gateway's Kubernetes end-to-end suite
  (`tests/gateway/e2e`), and OpenViking's full server with a model.
