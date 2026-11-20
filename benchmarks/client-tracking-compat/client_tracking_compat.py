#!/usr/bin/env python3
"""Client-side caching the way Frappe uses it: `CLIENT TRACKING ON REDIRECT <id> NOLOOP`, with the
invalidations arriving as messages on `__redis__:invalidate` on a second (RESP2) connection.

A cache connection reads keys; a subscriber connection is subscribed to `__redis__:invalidate`; the
cache connection turns tracking on with REDIRECT to the subscriber's `CLIENT ID` and NOLOOP, so that
its own writes do not invalidate it. Each check is something the cache depends on: the replies of
the tracking commands, which write invalidates which key (and which does not), that a key is sent
once until it is read again, flushes, expiry, BCAST with a prefix, `CLIENT TRACKINGINFO`, and a
redirect target that has gone away (RESP3).

Stdlib only. Usage: client_tracking_compat.py --port 6379 --junitxml out.xml
"""
import argparse
import functools
import os
import sys
import time

here = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(here, "..", "ci"))
from resp import Conn, Checks, Timeout, Err, eq  # noqa: E402
from compat_junit import write_junit  # noqa: E402

CHANNEL = "__redis__:invalidate"


class Rig:
    """Three connections: `cache` (tracked), `sub` (subscribed to the invalidation channel) and
    `other` (another client that writes)."""

    def __init__(self, port):
        self.port = port
        self.cache, self.sub, self.other = Conn(port), Conn(port), Conn(port)
        self.other.cmd("FLUSHALL")
        self.sub_id = self.sub.cmd("CLIENT", "ID")
        ack = self.sub.cmd("SUBSCRIBE", CHANNEL)
        eq(ack, ["subscribe", CHANNEL, 1], "subscribe reply")

    def track(self, *opts):
        return self.cache.cmd("CLIENT", "TRACKING", "ON", "REDIRECT", self.sub_id, *opts)

    def invalidated(self, wait=0.6):
        """The keys the subscriber was told about within `wait` seconds (a None payload, a flush,
        shows as '<flush>')."""
        keys, end = [], time.monotonic() + wait
        while True:
            left = end - time.monotonic()
            if left <= 0:
                return keys
            try:
                msg = self.sub.read(left)
            except Timeout:
                return keys
            eq(msg[:2], ["message", CHANNEL], "push kind")
            if msg[2] is None:
                keys.append("<flush>")
            else:
                keys.extend(msg[2])

    def close(self):
        for c in (self.cache, self.sub, self.other):
            c.close()


def with_rig(port):
    def deco(fn):
        @functools.wraps(fn)
        def run():
            r = Rig(port)
            try:
                fn(r)
            finally:
                r.close()
        return run
    return deco


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=6379)
    ap.add_argument("--junitxml")
    a = ap.parse_args()
    port = a.port
    checks = Checks()
    t0 = time.monotonic()

    @with_rig(port)
    def tracking_on_redirect_noloop(r):
        eq(r.track("NOLOOP"), "OK", "CLIENT TRACKING ON REDIRECT NOLOOP")

    @with_rig(port)
    def other_client_write_invalidates(r):
        r.track("NOLOOP")
        r.cache.cmd("GET", "frappe:a")
        eq(r.other.cmd("SET", "frappe:a", "1"), "OK")
        eq(r.invalidated(), ["frappe:a"], "invalidated keys")

    @with_rig(port)
    def own_write_does_not_invalidate_noloop(r):
        r.track("NOLOOP")
        r.cache.cmd("GET", "frappe:b")
        eq(r.cache.cmd("SET", "frappe:b", "mine"), "OK")
        eq(r.invalidated(), [], "invalidated keys after the client's own write")

    @with_rig(port)
    def own_write_invalidates_without_noloop(r):
        r.track()
        r.cache.cmd("GET", "frappe:b2")
        eq(r.cache.cmd("SET", "frappe:b2", "mine"), "OK")
        eq(r.invalidated(), ["frappe:b2"], "invalidated keys after the client's own write")

    @with_rig(port)
    def key_sent_once_until_read_again(r):
        r.track("NOLOOP")
        r.cache.cmd("GET", "frappe:c")
        r.other.cmd("SET", "frappe:c", "1")
        eq(r.invalidated(), ["frappe:c"], "first write")
        r.other.cmd("SET", "frappe:c", "2")
        eq(r.invalidated(), [], "second write, key not read in between")
        r.cache.cmd("GET", "frappe:c")
        r.other.cmd("SET", "frappe:c", "3")
        eq(r.invalidated(), ["frappe:c"], "write after reading it again")

    @with_rig(port)
    def untracked_key_is_not_sent(r):
        r.track("NOLOOP")
        r.cache.cmd("GET", "frappe:d")
        r.other.cmd("SET", "frappe:other", "1")
        eq(r.invalidated(), [], "write to a key never read")

    @with_rig(port)
    def mset_and_del_invalidate_every_key(r):
        r.track("NOLOOP")
        r.cache.cmd("MGET", "m:1", "m:2", "m:3")
        r.other.cmd("MSET", "m:1", "a", "m:2", "b")
        eq(sorted(r.invalidated()), ["m:1", "m:2"], "MSET")
        r.cache.cmd("MGET", "m:1", "m:2", "m:3")
        r.other.cmd("DEL", "m:1", "m:2", "m:9")
        eq(sorted(r.invalidated()), ["m:1", "m:2"], "DEL (a key that is not there sends nothing)")

    @with_rig(port)
    def hash_write_invalidates_its_key(r):
        r.track("NOLOOP")
        r.cache.cmd("HGETALL", "h:1")
        r.other.cmd("HSET", "h:1", "f", "v")
        eq(r.invalidated(), ["h:1"], "HSET")

    @with_rig(port)
    def expire_invalidates(r):
        r.other.cmd("SET", "e:1", "v", "PX", "200")
        r.track("NOLOOP")
        r.cache.cmd("GET", "e:1")
        time.sleep(0.5)
        # Reading an expired key is what deletes it when the server has not got to it yet.
        r.other.cmd("GET", "e:1")
        eq(r.invalidated(3.0), ["e:1"], "expiry")

    @with_rig(port)
    def flushall_sends_a_null_invalidation(r):
        r.track("NOLOOP")
        r.cache.cmd("GET", "f:1")
        r.other.cmd("SET", "f:1", "v")
        r.invalidated()
        r.cache.cmd("GET", "f:1")
        r.other.cmd("FLUSHALL")
        eq(r.invalidated(), ["<flush>"], "FLUSHALL")

    @with_rig(port)
    def tracking_off_stops_invalidations(r):
        r.track("NOLOOP")
        r.cache.cmd("GET", "o:1")
        eq(r.cache.cmd("CLIENT", "TRACKING", "OFF"), "OK")
        r.other.cmd("SET", "o:1", "v")
        eq(r.invalidated(), [], "after TRACKING OFF")

    @with_rig(port)
    def trackinginfo_reports_redirect_and_noloop(r):
        r.track("NOLOOP")
        info = r.cache.cmd("CLIENT", "TRACKINGINFO")
        flat = dict(zip(info[0::2], info[1::2]))
        assert "on" in flat["flags"] and "noloop" in flat["flags"], "flags %r" % (flat["flags"],)
        eq(flat["redirect"], r.sub_id, "redirect")
        eq(flat["prefixes"], [], "prefixes")

    @with_rig(port)
    def redirect_to_a_missing_client_is_an_error(r):
        got = r.cache.cmd("CLIENT", "TRACKING", "ON", "REDIRECT", 999999999, "NOLOOP")
        assert isinstance(got, Err), "got %r, want an error" % (got,)
        assert "redirect" in got.lower() and "exist" in got.lower(), got

    @with_rig(port)
    def bcast_prefix_noloop(r):
        eq(r.cache.cmd("CLIENT", "TRACKING", "ON", "BCAST", "PREFIX", "user:", "REDIRECT", r.sub_id, "NOLOOP"), "OK")
        r.other.cmd("SET", "user:1", "x")
        r.other.cmd("SET", "other:1", "x")
        r.cache.cmd("SET", "user:2", "mine")
        eq(r.invalidated(), ["user:1"], "BCAST user: keys, not other keys, not the client's own")

    @with_rig(port)
    def bcast_overlapping_prefixes_are_an_error(r):
        eq(r.cache.cmd("CLIENT", "TRACKING", "ON", "BCAST", "PREFIX", "ab", "REDIRECT", r.sub_id), "OK")
        got = r.cache.cmd("CLIENT", "TRACKING", "ON", "BCAST", "PREFIX", "abc", "REDIRECT", r.sub_id)
        assert isinstance(got, Err), "got %r, want an error for overlapping prefixes" % (got,)

    def resp3_redirect_target_gone():
        """With RESP3 the tracked connection is told, by a push, when its redirect target has gone."""
        cache, other = Conn(port, resp3=True), Conn(port)
        sub = Conn(port)
        try:
            sid = sub.cmd("CLIENT", "ID")
            eq(cache.cmd("CLIENT", "TRACKING", "ON", "REDIRECT", sid, "NOLOOP"), "OK")
            cache.cmd("GET", "g:1")
            sub.close()
            time.sleep(0.3)
            other.cmd("SET", "g:1", "v")
            seen = None
            try:
                seen = cache.read(2.0)
            except Timeout:
                pass
            assert seen and seen[0] == "tracking-redir-broken" and seen[1] == sid, \
                "want a tracking-redir-broken push for %s, got %r" % (sid, seen)
        finally:
            cache.close()
            other.close()

    @with_rig(port)
    def resp3_inline_invalidation(r):
        c3 = Conn(port, resp3=True)
        try:
            eq(c3.cmd("CLIENT", "TRACKING", "ON", "NOLOOP"), "OK")
            c3.cmd("GET", "r3:1")
            r.other.cmd("SET", "r3:1", "v")
            push = c3.read(2.0)
            eq(push, ["invalidate", ["r3:1"]], "RESP3 push")
        finally:
            c3.close()

    for fn in (tracking_on_redirect_noloop, other_client_write_invalidates,
               own_write_does_not_invalidate_noloop, own_write_invalidates_without_noloop,
               key_sent_once_until_read_again, untracked_key_is_not_sent,
               mset_and_del_invalidate_every_key, hash_write_invalidates_its_key, expire_invalidates,
               flushall_sends_a_null_invalidation, tracking_off_stops_invalidations,
               trackinginfo_reports_redirect_and_noloop, redirect_to_a_missing_client_is_an_error,
               bcast_prefix_noloop, bcast_overlapping_prefixes_are_an_error, resp3_inline_invalidation):
        checks.run("Tracking: " + fn.__name__.replace("_", " "), fn)
    checks.run("Tracking: resp3 redirect target gone", resp3_redirect_target_gone)

    print("%d checks, %d failed" % (len(checks.rows), checks.failed()))
    if a.junitxml:
        write_junit(a.junitxml, "client-tracking", checks.rows, time.monotonic() - t0)
    return 0


if __name__ == "__main__":
    sys.exit(main())
