#!/usr/bin/env python3
"""
Blocking-command battery, run against a live kividb on either I/O model.

Why this exists as a separate smoke script rather than only as pytest cases:
the blocking family is the one part of the command surface whose behaviour
depends on the *runtime* underneath the connection loop, not just on the
handler. It used to bridge an async wait out of synchronous dispatch with
`tokio::task::block_in_place`, which panics on a current-thread runtime — so
every one of these commands killed the connection under `--io-model uring`
while passing cleanly under `--io-model tokio`. A battery that runs
identically against both models is what makes that class of divergence
visible.

Each check asserts on three things a naive test misses:

  * that the reply *arrives* (the connection did not die),
  * that it arrives only *after* the pushing client acts (we really blocked,
    rather than returning a fast-path answer),
  * that the connection is still usable afterwards.

Usage: python smoke/blocking_matrix.py --port 6380 [--label uring]
"""
from __future__ import annotations

import argparse
import sys
import threading
import time

import redis

FAILURES: list[str] = []
CHECKS = 0


def check(name: str, fn) -> None:
    global CHECKS
    CHECKS += 1
    try:
        fn()
        print(f"  ok    {name}")
    except Exception as exc:  # noqa: BLE001 - smoke script reports, never raises
        FAILURES.append(f"{name}: {exc!r}")
        print(f"  FAIL  {name}: {exc!r}")


def conn(port: int, protocol: int = 2) -> redis.Redis:
    return redis.Redis(
        host="127.0.0.1", port=port, protocol=protocol,
        decode_responses=True, socket_timeout=15,
    )


def seq(v):
    """Normalise a sequence reply for comparison.

    redis-py hands back a tuple under RESP2 and a list under RESP3 for the same
    server bytes (`BLPOP`, `BZPOPMIN`, …). Both are correct client-side
    containers, so comparing as lists keeps these checks about the *server*.
    """
    return list(v) if isinstance(v, (list, tuple)) else v


def _is_entry_list(v) -> bool:
    """True for a list of `(id, {field: value})` stream entries."""
    if not isinstance(v, (list, tuple)):
        return False
    if not v:
        return True
    head = v[0]
    return isinstance(head, (list, tuple)) and len(head) == 2 and isinstance(head[1], dict)


def stream_entries(reply, key: str):
    """Pull the entry list for `key` out of an XREAD/XREADGROUP reply.

    Three client-side shapes for the same server bytes:

      RESP2  `[[name, [(id, fields), ...]], ...]`
      RESP3  `{name: [[(id, fields), ...]]}`   <- note the extra list level

    That extra level is redis-py's own RESP3 callback, not something on the
    wire: kividb's bytes for both protocols were diffed against real Redis 7
    and are identical (`%1` map + `*n` entry array under RESP3, `*1` nested
    arrays under RESP2). So this unwraps the client container rather than
    papering over a server difference.
    """
    if not reply:
        return []
    if isinstance(reply, dict):
        v = reply.get(key, [])
    else:
        v = []
        for item in reply:
            if isinstance(item, (list, tuple)) and len(item) == 2 and item[0] == key:
                v = item[1]
                break
    if not _is_entry_list(v) and len(v) == 1:
        v = v[0]
    return v


def delayed(fn, delay: float = 0.25) -> threading.Thread:
    """Run `fn` after `delay`s on another thread, so the blocked client is
    genuinely parked when the wake-up arrives."""
    t = threading.Thread(target=lambda: (time.sleep(delay), fn()), daemon=True)
    t.start()
    return t


def run(port: int, protocol: int) -> None:
    r = conn(port, protocol)
    w = conn(port, protocol)
    r.flushall()

    # ── BLPOP / BRPOP ────────────────────────────────────────────────────────
    def blpop_wakes():
        delayed(lambda: w.rpush("bl:1", "v1"))
        t0 = time.monotonic()
        got = r.blpop(["bl:1"], timeout=5)
        assert seq(got) == ["bl:1", "v1"], got
        assert time.monotonic() - t0 > 0.1, "returned too fast — did it block?"

    check("BLPOP wakes on RPUSH", blpop_wakes)

    def brpop_wakes():
        delayed(lambda: w.rpush("bl:2", "v2"))
        got = r.brpop(["bl:2"], timeout=5)
        assert seq(got) == ["bl:2", "v2"], got

    check("BRPOP wakes on RPUSH", brpop_wakes)

    def blpop_timeout():
        t0 = time.monotonic()
        got = r.blpop(["bl:nope"], timeout=1)
        assert got is None, got
        elapsed = time.monotonic() - t0
        assert 0.7 < elapsed < 3.0, f"timeout took {elapsed:.2f}s"

    check("BLPOP honours its timeout", blpop_timeout)

    def blpop_multikey():
        # The reply must name the key that actually fired, not the first listed.
        delayed(lambda: w.rpush("bl:m3", "third"))
        got = r.blpop(["bl:m1", "bl:m2", "bl:m3"], timeout=5)
        assert seq(got) == ["bl:m3", "third"], got

    check("BLPOP multi-key reports the firing key", blpop_multikey)

    def blpop_existing_data_is_fast():
        w.rpush("bl:fast", "now")
        t0 = time.monotonic()
        got = r.blpop(["bl:fast"], timeout=5)
        assert seq(got) == ["bl:fast", "now"], got
        assert time.monotonic() - t0 < 0.5, "fast path should not block"

    check("BLPOP returns immediately when data exists", blpop_existing_data_is_fast)

    # ── BLMOVE / BRPOPLPUSH ──────────────────────────────────────────────────
    def blmove_wakes():
        delayed(lambda: w.rpush("bm:src", "x"))
        got = r.blmove("bm:src", "bm:dst", timeout=5, src="LEFT", dest="RIGHT")
        assert got == "x", got
        assert r.lrange("bm:dst", 0, -1) == ["x"], r.lrange("bm:dst", 0, -1)

    check("BLMOVE wakes and lands in the destination", blmove_wakes)

    def brpoplpush_wakes():
        delayed(lambda: w.rpush("bp:src", "y"))
        got = r.brpoplpush("bp:src", "bp:dst", timeout=5)
        assert got == "y", got
        assert r.lrange("bp:dst", 0, -1) == ["y"], r.lrange("bp:dst", 0, -1)

    check("BRPOPLPUSH wakes and lands in the destination", brpoplpush_wakes)

    def blmove_timeout():
        assert r.blmove("bm:none", "bm:none2", timeout=1, src="LEFT", dest="LEFT") is None

    check("BLMOVE honours its timeout", blmove_timeout)

    # ── BLMPOP ───────────────────────────────────────────────────────────────
    def blmpop_wakes():
        delayed(lambda: w.rpush("bmp:1", "a"))
        got = r.blmpop(5, 1, "bmp:1", direction="LEFT")
        assert got is not None and got[0] == "bmp:1", got
        assert seq(got[1]) == ["a"], got

    check("BLMPOP wakes on RPUSH", blmpop_wakes)

    def blmpop_timeout():
        assert r.blmpop(1, 1, "bmp:none", direction="LEFT") is None

    check("BLMPOP honours its timeout", blmpop_timeout)

    # ── BZPOPMIN / BZPOPMAX / BZMPOP ─────────────────────────────────────────
    def bzpopmin_wakes():
        delayed(lambda: w.zadd("bz:1", {"m": 1.5}))
        got = r.bzpopmin(["bz:1"], timeout=5)
        assert seq(got) == ["bz:1", "m", 1.5], got

    check("BZPOPMIN wakes on ZADD", bzpopmin_wakes)

    def bzpopmax_wakes():
        delayed(lambda: w.zadd("bz:2", {"n": 2.5}))
        got = r.bzpopmax(["bz:2"], timeout=5)
        assert seq(got) == ["bz:2", "n", 2.5], got

    check("BZPOPMAX wakes on ZADD", bzpopmax_wakes)

    def bzpopmin_timeout():
        assert r.bzpopmin(["bz:none"], timeout=1) is None

    check("BZPOPMIN honours its timeout", bzpopmin_timeout)

    def bzmpop_wakes():
        # Shape must be [key, [[member, score], ...]] — nested, unlike BZPOPMIN.
        delayed(lambda: w.zadd("bzm:1", {"q": 3.0}))
        got = r.bzmpop(5, 1, ["bzm:1"], min=True)
        assert got is not None, got
        key, pairs = got[0], got[1]
        assert key == "bzm:1", got
        # RESP2 carries the score as a bulk string, RESP3 as a double — both
        # are correct on the wire, so compare numerically.
        assert [[m, float(s)] for m, s in pairs] == [["q", 3.0]], pairs

    check("BZMPOP wakes with its nested reply shape", bzmpop_wakes)

    def bzmpop_timeout():
        assert r.bzmpop(1, 1, ["bzm:none"], min=True) is None

    check("BZMPOP honours its timeout", bzmpop_timeout)

    # ── XREAD BLOCK ──────────────────────────────────────────────────────────
    def xread_block_dollar():
        # `$` means "entries added after this command arrived". Resolving it a
        # second time on wake-up would skip the very entry that woke us, so this
        # is the check that the resolved IDs survive the park.
        w.xadd("xs:1", {"before": "1"})
        delayed(lambda: w.xadd("xs:1", {"after": "1"}))
        got = r.xread({"xs:1": "$"}, block=5000, count=10)
        assert got, "XREAD BLOCK $ returned nothing"
        entries = stream_entries(got, "xs:1")
        fields = [e[1] for e in entries]
        assert {"after": "1"} in fields, fields
        assert {"before": "1"} not in fields, "must not replay pre-existing entries"

    check("XREAD BLOCK $ sees only entries added after it parked", xread_block_dollar)

    def xread_block_timeout():
        assert r.xread({"xs:none": "$"}, block=1000) in (None, [], {}), "expected empty"

    check("XREAD BLOCK honours its timeout", xread_block_timeout)

    def xread_block_from_zero():
        w.xadd("xs:2", {"a": "1"})
        got = r.xread({"xs:2": "0"}, block=5000, count=10)
        assert got, "existing entries should return immediately"

    check("XREAD BLOCK returns existing entries immediately", xread_block_from_zero)

    # ── XREADGROUP BLOCK ─────────────────────────────────────────────────────
    def xreadgroup_block():
        w.xadd("xg:1", {"seed": "1"})
        try:
            w.xgroup_create("xg:1", "g1", id="$")
        except redis.ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise
        delayed(lambda: w.xadd("xg:1", {"live": "1"}))
        got = r.xreadgroup("g1", "c1", {"xg:1": ">"}, block=5000, count=10)
        assert got, "XREADGROUP BLOCK returned nothing"
        entries = stream_entries(got, "xg:1")
        assert [e[1] for e in entries] == [{"live": "1"}], entries

    check("XREADGROUP BLOCK wakes on XADD", xreadgroup_block)

    def xreadgroup_block_timeout():
        w.xadd("xg:2", {"seed": "1"})
        try:
            w.xgroup_create("xg:2", "g2", id="$")
        except redis.ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise
        got = r.xreadgroup("g2", "c1", {"xg:2": ">"}, block=1000, count=10)
        assert got in (None, [], {}), got

    check("XREADGROUP BLOCK honours its timeout", xreadgroup_block_timeout)

    # ── Connection health and interaction with other features ────────────────
    def connection_survives_blocking():
        # The original io_uring bug killed the connection outright, so every
        # check above could "pass" as a timeout while the socket was dead.
        # Prove the connection still works after a block.
        r.blpop(["surv:none"], timeout=1)
        assert r.ping() is True
        assert r.set("surv:k", "v") is True
        assert r.get("surv:k") == "v"

    check("connection is still usable after a block", connection_survives_blocking)

    def pipelined_reply_not_held_hostage():
        # A pipelined SET before a BLPOP must not wait for the BLPOP to finish.
        raw = conn(port, protocol)
        sock = raw.connection_pool.get_connection("_")
        try:
            sock.send_command("SET", "pipe:k", "pipe:v")
            sock.send_command("BLPOP", "pipe:none", "2")
            t0 = time.monotonic()
            first = sock.read_response()
            first_at = time.monotonic() - t0
            assert first in (b"OK", "OK", True), first
            assert first_at < 1.0, f"+OK held for {first_at:.2f}s behind the BLPOP"
            second = sock.read_response()
            assert second is None, second
        finally:
            raw.connection_pool.release(sock)

    check("pipelined reply is flushed before parking", pipelined_reply_not_held_hostage)

    def blocking_in_multi_does_not_block():
        # Redis: blocking commands inside MULTI never block — they return their
        # empty-handed reply immediately.
        p = r.pipeline(transaction=True)
        p.blpop(["multi:none"], timeout=0)
        t0 = time.monotonic()
        out = p.execute()
        elapsed = time.monotonic() - t0
        assert elapsed < 2.0, f"BLPOP inside MULTI blocked for {elapsed:.2f}s"
        assert out == [None], out

    check("BLPOP inside MULTI returns immediately", blocking_in_multi_does_not_block)

    def two_blocked_clients_are_independent():
        a, b = conn(port, protocol), conn(port, protocol)
        results: dict[str, object] = {}

        def wait(name: str, c: redis.Redis, key: str):
            results[name] = c.blpop([key], timeout=6)

        ta = threading.Thread(target=wait, args=("a", a, "multi:a"), daemon=True)
        tb = threading.Thread(target=wait, args=("b", b, "multi:b"), daemon=True)
        ta.start()
        tb.start()
        time.sleep(0.3)
        w.rpush("multi:a", "for-a")
        w.rpush("multi:b", "for-b")
        ta.join(8)
        tb.join(8)
        assert seq(results.get("a")) == ["multi:a", "for-a"], results
        assert seq(results.get("b")) == ["multi:b", "for-b"], results

    check("two clients block concurrently without interfering", two_blocked_clients_are_independent)

    def blocked_client_does_not_stall_others():
        # The whole point of not blocking the worker thread: another connection
        # must stay responsive while this one is parked indefinitely.
        parked = conn(port, protocol)
        done = threading.Event()

        def park():
            parked.blpop(["stall:none"], timeout=4)
            done.set()

        threading.Thread(target=park, daemon=True).start()
        time.sleep(0.3)
        other = conn(port, protocol)
        t0 = time.monotonic()
        for i in range(50):
            other.set(f"stall:{i}", i)
        elapsed = time.monotonic() - t0
        assert elapsed < 3.0, f"50 SETs took {elapsed:.2f}s while a client was parked"
        assert not done.is_set(), "the parked client should still be waiting"

    check("a parked client does not stall other connections", blocked_client_does_not_stall_others)

    def blocked_clients_reported():
        parked = conn(port, protocol)
        threading.Thread(
            target=lambda: parked.blpop(["info:none"], timeout=3), daemon=True
        ).start()
        time.sleep(0.5)
        info = r.info("clients")
        assert int(info.get("blocked_clients", 0)) >= 1, info.get("blocked_clients")

    check("INFO reports blocked_clients while parked", blocked_clients_reported)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=6380)
    ap.add_argument("--label", default="")
    ap.add_argument("--protocol", type=int, default=0, help="2, 3, or 0 for both")
    args = ap.parse_args()

    protocols = [2, 3] if args.protocol == 0 else [args.protocol]
    for proto in protocols:
        header = f"blocking matrix (RESP{proto})"
        if args.label:
            header += f" [{args.label}]"
        print(f"\n== {header} ==")
        run(args.port, proto)

    print(f"\n{CHECKS - len(FAILURES)}/{CHECKS} checks passed")
    if FAILURES:
        print("\nFailures:")
        for f in FAILURES:
            print(f"  - {f}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
