#!/usr/bin/env python3
"""The rate limiters' scripts, called the way their gateways call them, with the replies Redis gives.

  Spring Cloud Gateway  `request_rate_limiter.lua` (spring-cloud-gateway v5.1.0): a token bucket in two
                        keys (`tokens`, `timestamp`), arguments rate, capacity, now, requested; calls
                        `redis.replicate_commands()`; answers `{allowed, tokens}`.
  Traefik               the token bucket of `pkg/middlewares/ratelimiter/lua.go` (traefik v3.7.17): one
                        hash, read with `HGETALL` and the values taken from positions 2 and 4 of the
                        reply (traefik/traefik#14297), so the order of its fields matters; answers three
                        strings.
  Kong                  the rate-limiting plugin's `incr`, and `expire` on the first hit (Kong 3.9.4,
                        `policies/init.lua`): `EVAL`/`EVALSHA` with one key; answers the count before.

Each check drives a fixed sequence (fixed clocks, so the answers do not depend on the time) and
compares every reply, the keys the script leaves, their values and their TTLs with what Redis 8.10
answers, written out here. Kong's is also run from four connections at once: every call must see a
count of its own (no two the same, none missing), which is what the scripts are for.

Stdlib only. Usage: ratelimit_scripts_compat.py --scg request_rate_limiter.lua --traefik lua.go
                    --port 6379 --junitxml out.xml
"""
import argparse
import hashlib
import os
import re
import sys
import threading
import time

here = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(here, "..", "ci"))
from resp import Conn, Checks, Err, eq  # noqa: E402
from compat_junit import write_junit  # noqa: E402

# Kong 3.9.4, kong/plugins/rate-limiting/policies/init.lua, `usage()`: the script as it is.
KONG = """local cache_key, expiration = KEYS[1], ARGV[1]
local result_incr = redis.call("incr", cache_key)
if result_incr == 1 then
  redis.call("expire", cache_key, expiration)
end

return result_incr - 1"""


def traefik_script(path):
    """AllowTokenBucketRaw from lua.go: the text between the backticks that follow it."""
    with open(path, encoding="utf-8") as f:
        src = f.read()
    m = re.search(r"AllowTokenBucketRaw\s*=\s*`([^`]*)`", src)
    assert m, "AllowTokenBucketRaw not found in " + path
    return m.group(1)


def ok(reply):
    assert not isinstance(reply, Err), reply
    return reply


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--scg", required=True)
    ap.add_argument("--traefik", required=True)
    ap.add_argument("--port", type=int, default=6379)
    ap.add_argument("--junitxml")
    a = ap.parse_args()
    port = a.port
    with open(a.scg, encoding="utf-8") as f:
        scg = f.read()
    traefik = traefik_script(a.traefik)
    checks = Checks()
    t0 = time.monotonic()

    def conn():
        c = Conn(port)
        c.cmd("FLUSHALL")
        return c

    # ---- Spring Cloud Gateway ---------------------------------------------------------------
    TOK, TS = "request_rate_limiter.{user1}.tokens", "request_rate_limiter.{user1}.timestamp"

    def scg_call(c, rate, cap, now, requested=1):
        args = [rate, cap] + ([now] if now is not None else []) + [requested]
        if now is None:
            # The script reads ARGV[3] as `now` and ARGV[4] as `requested`: a missing clock is an
            # empty argument, as the gateway sends none and Lua's tonumber("") is nil.
            args = [rate, cap, "", requested]
        return ok(c.cmd("EVAL", scg, 2, TOK, TS, *args))

    def scg_bucket_sequence():
        c = conn()
        got = [scg_call(c, 2, 4, 1000) for _ in range(5)]
        eq(got, [[1, 3], [1, 2], [1, 1], [1, 0], [0, 0]], "five requests, capacity 4")
        eq([scg_call(c, 2, 4, 1001) for _ in range(3)], [[1, 1], [1, 0], [0, 0]], "one second later: 2 tokens")
        eq(scg_call(c, 2, 4, 1010), [1, 3], "a long pause refills to the capacity only")

    def scg_requested_tokens():
        c = conn()
        eq(scg_call(c, 1, 10, 500, requested=4), [1, 6], "4 of 10")
        eq(scg_call(c, 1, 10, 500, requested=7), [0, 6], "7 of the 6 left is refused and takes nothing")
        eq(scg_call(c, 1, 10, 502, requested=7), [1, 1], "after 2 s: 8 tokens, 7 taken")

    def scg_keys_and_ttl():
        c = conn()
        scg_call(c, 2, 4, 1000)
        eq(c.cmd("GET", TOK), "3", "tokens key")
        eq(c.cmd("GET", TS), "1000", "timestamp key")
        # fill_time = capacity / rate = 2 s, ttl = floor(2 * 2) = 4 s.
        for k in (TOK, TS):
            ttl = c.cmd("TTL", k)
            assert 0 < ttl <= 4, "TTL of %s is %r, want 1..4" % (k, ttl)

    def scg_fractional_tokens():
        c = conn()
        eq(scg_call(c, 0.5, 3, 100), [1, 2], "first")
        r = scg_call(c, 0.5, 3, 101)  # 2 + 0.5 filled, 1 taken: 1.5 tokens
        eq(r, [1, 1], "the reply truncates 1.5 to an integer")
        eq(c.cmd("GET", TOK), "1.5", "the key keeps the fraction")
        eq(c.cmd("TTL", TOK) <= 12 and c.cmd("TTL", TOK) > 0, True, "ttl = floor(2 * 3 / 0.5) = 12")

    def scg_clock_from_server():
        c = conn()
        r = scg_call(c, 1, 5, None)
        eq(r, [1, 4], "no clock given: the script takes TIME")
        ts = int(c.cmd("GET", TS))
        assert abs(ts - time.time()) < 30, "timestamp %d is not the server's clock %d" % (ts, time.time())

    def scg_evalsha():
        c = conn()
        sha = c.cmd("SCRIPT", "LOAD", scg)
        eq(sha, hashlib.sha1(scg.encode()).hexdigest(), "SCRIPT LOAD sha1")
        eq(ok(c.cmd("EVALSHA", sha, 2, TOK, TS, 2, 4, 1000, 1)), [1, 3], "EVALSHA")
        eq(ok(c.cmd("EVALSHA", sha, 2, TOK, TS, 2, 4, 1000, 1)), [1, 2], "EVALSHA again")

    # ---- Traefik -----------------------------------------------------------------------------
    KEY = "traefik:{client-1}"

    def tf_call(c, limit, burst, ttl, t, max_delay):
        """The reply as (allowed, wait seconds, tokens): the numbers parsed, as Traefik parses them.
        Redis's Lua writes 1 where KiviDB's Lua 5.4 writes 1.0 for the result of a division
        (docs/SCRIPTS.md, "Lua 5.4 number rules"): a known difference of the text, not of the value."""
        r = ok(c.cmd("EVAL", traefik, 1, KEY, limit, burst, ttl, t, max_delay))
        assert len(r) == 3 and r[0] == "true", r
        return [r[0], float(r[1]), float(r[2])]

    def tf_bucket_sequence():
        c = conn()
        seq = [tf_call(c, 1, 5, 10, 1000, 0) for _ in range(6)]
        eq(seq, [["true", 0, 4], ["true", 0, 3], ["true", 0, 2], ["true", 0, 1],
                 ["true", 0, 0], ["true", 1, 0]], "burst of 5, then over the limit")
        eq(tf_call(c, 1, 5, 10, 1000.5, 0), ["true", 0.5, 0.5], "half a second later: 0.5 refilled, 1 asked, over the limit")
        eq(tf_call(c, 1, 5, 10, 1003, 0), ["true", 0, 2], "2.5 s later: 3 tokens, 1 taken")

    def tf_delay_allowed():
        c = conn()
        for _ in range(5):
            tf_call(c, 1, 5, 10, 1000, 5)
        eq(tf_call(c, 1, 5, 10, 1000, 5), ["true", 1, -1], "over the limit but inside max_delay")
        eq(tf_call(c, 1, 5, 10, 1000, 5), ["true", 2, -2], "and again")

    def tf_field_order_and_ttl():
        c = conn()
        for t in (1000, 1000.25, 1001, 1002.5):
            tf_call(c, 1, 5, 10, t, 0)
            got = c.cmd("HGETALL", KEY)
            assert len(got) == 4 and got[0] == "last" and got[2] == "tokens", \
                "HGETALL must list `last` then `tokens` (positions 2 and 4 are read): %r" % (got,)
        eq(c.cmd("HGET", KEY, "last"), "1002.5", "last")
        ttl = c.cmd("TTL", KEY)
        assert 0 < ttl <= 10, "TTL %r" % (ttl,)

    def tf_field_order_many_buckets():
        c = conn()
        for i in range(200):
            key = "traefik:{b%d}" % i
            ok(c.cmd("EVAL", traefik, 1, key, 1, 5, 10, 1000 + i, 0))
            ok(c.cmd("EVAL", traefik, 1, key, 1, 5, 10, 1000 + i, 0))
            got = c.cmd("HGETALL", key)
            assert got[0] == "last" and got[2] == "tokens", "bucket %d: %r" % (i, got)

    def tf_evalsha():
        c = conn()
        sha = c.cmd("SCRIPT", "LOAD", traefik)
        eq(sha, hashlib.sha1(traefik.encode()).hexdigest(), "SCRIPT LOAD sha1")
        eq(ok(c.cmd("EVALSHA", sha, 1, KEY, 1, 5, 10, 1000, 0)), ["true", "0", "4"], "EVALSHA")
        eq(ok(c.cmd("EVALSHA", sha, 1, KEY, 1, 5, 10, 1000, 0)), ["true", "0", "3"], "EVALSHA again")

    # ---- Kong --------------------------------------------------------------------------------
    KKEY = "ratelimit:6f9c5f5c:0d3c6e1a:{consumer-7}:1763571600000:minute"

    def kong_counts():
        c = conn()
        got = [ok(c.cmd("EVAL", KONG, 1, KKEY, 60)) for _ in range(5)]
        eq(got, [0, 1, 2, 3, 4], "the count before each hit")
        eq(c.cmd("GET", KKEY), "5", "the counter")

    def kong_ttl_set_on_first_hit_only():
        c = conn()
        ok(c.cmd("EVAL", KONG, 1, KKEY, 60))
        ttl = c.cmd("TTL", KKEY)
        assert 58 <= ttl <= 60, "TTL after the first hit: %r" % (ttl,)
        c.cmd("EXPIRE", KKEY, 30)
        ok(c.cmd("EVAL", KONG, 1, KKEY, 60))
        ttl = c.cmd("TTL", KKEY)
        assert 0 < ttl <= 30, "a later hit must not reset the TTL: %r" % (ttl,)

    def kong_new_period_starts_again():
        c = conn()
        ok(c.cmd("EVAL", KONG, 1, KKEY, 1))
        ok(c.cmd("EVAL", KONG, 1, KKEY, 1))
        time.sleep(1.3)
        eq(ok(c.cmd("EVAL", KONG, 1, KKEY, 1)), 0, "after the key expired")

    def kong_evalsha():
        c = conn()
        sha = c.cmd("SCRIPT", "LOAD", KONG)
        eq(sha, hashlib.sha1(KONG.encode()).hexdigest(), "SCRIPT LOAD sha1")
        eq([ok(c.cmd("EVALSHA", sha, 1, KKEY, 60)) for _ in range(3)], [0, 1, 2], "EVALSHA")
        eq(c.cmd("SCRIPT", "EXISTS", sha), [1], "SCRIPT EXISTS")
        got = c.cmd("EVALSHA", "0" * 40, 1, KKEY, 60)
        assert isinstance(got, Err) and got.startswith("NOSCRIPT"), "unknown sha: %r" % (got,)

    def kong_pipelined():
        c = conn()
        n = 500
        c.sock.sendall(b"".join(Conn.encode(["EVAL", KONG, 1, KKEY, 60]) for _ in range(n)))
        got = [c.read() for _ in range(n)]
        eq(got, list(range(n)), "pipelined counts")

    def kong_concurrent_connections():
        c = conn()
        results, errors = [], []

        def worker():
            try:
                w = Conn(port)
                mine = [ok(w.cmd("EVAL", KONG, 1, KKEY, 60)) for _ in range(250)]
                results.extend(mine)
                w.close()
            except Exception as e:  # noqa: BLE001
                errors.append(repr(e))

        threads = [threading.Thread(target=worker) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert not errors, errors
        eq(sorted(results), list(range(1000)), "the counts seen by 4 connections of 250 hits")
        eq(c.cmd("GET", KKEY), "1000", "the counter")

    def kong_many_keys():
        c = conn()
        keys = ["ratelimit:r:s:c%d:1763571600000:minute" % i for i in range(300)]
        for k in keys:
            eq(ok(c.cmd("EVAL", KONG, 1, k, 60)), 0, k)
        for k in keys[:100]:
            eq(ok(c.cmd("EVAL", KONG, 1, k, 60)), 1, k)
        eq(c.cmd("DBSIZE"), 300, "keys")

    for prefix, fns in (
        ("Spring Cloud Gateway", (scg_bucket_sequence, scg_requested_tokens, scg_keys_and_ttl,
                                  scg_fractional_tokens, scg_clock_from_server, scg_evalsha)),
        ("Traefik", (tf_bucket_sequence, tf_delay_allowed, tf_field_order_and_ttl,
                     tf_field_order_many_buckets, tf_evalsha)),
        ("Kong", (kong_counts, kong_ttl_set_on_first_hit_only, kong_new_period_starts_again, kong_evalsha,
                  kong_pipelined, kong_concurrent_connections, kong_many_keys)),
    ):
        for fn in fns:
            name = fn.__name__.split("_", 1)[1].replace("_", " ")
            checks.run("%s: %s" % (prefix, name), fn)

    print("%d checks, %d failed" % (len(checks.rows), checks.failed()))
    if a.junitxml:
        write_junit(a.junitxml, "ratelimit-scripts", checks.rows, time.monotonic() - t0)
    return 0


if __name__ == "__main__":
    sys.exit(main())
