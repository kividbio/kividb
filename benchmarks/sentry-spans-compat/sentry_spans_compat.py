#!/usr/bin/env python3
"""Sentry's span buffer script, `src/sentry/scripts/spans/add-buffer.lua` (getsentry/sentry at the
commit pinned in ci.sh), run as Sentry runs it: `EVAL` with one key `<project>:<trace>` and the nine
arguments the script documents, then the span ids. It touches keys it is not given (all under the
`{<project>:<trace>}` hash tag, as the script says: it is written for a cluster).

The checks replay what the buffer does with a trace: a subsegment arriving, a parent arriving after
its children and absorbing them (counters, byte counts, the member sets, the redirect hash), the
root flag, a second subsegment finding its parent through the redirect, a segment over its byte
limit and one being flushed being detached under their salt, a merge of more members than Lua's
`unpack` takes at once (the script's SSCAN batches), and the shape of the reply: the types of the six
elements, which is what Sentry's Python client indexes into, and the names of the timing and
metric tables. Timings themselves are not compared.

Stdlib only. Usage: sentry_spans_compat.py --script add-buffer.lua --port 6379 --junitxml out.xml
"""
import argparse
import hashlib
import os
import sys
import time

here = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(here, "..", "ci"))
from resp import Conn, Checks, Err, eq  # noqa: E402
from compat_junit import write_junit  # noqa: E402

TRACE = "1:0123456789abcdef0123456789abcdef"
TAG = "{%s}" % TRACE
SPAN = "span-buf:s:%s:" % TAG          # + the segment's top span id
IC = "span-buf:ic:" + SPAN             # spans ingested
IBC = "span-buf:ibc:" + SPAN           # bytes ingested
MK = "span-buf:mk:%s:" % TAG           # members
SSR = "span-buf:ssr:%s" % TAG          # redirects
TIMEOUT = 300
NOT_SAMPLED = 1000000007               # metrics_sample_rate: a microsecond is below it, never 0 modulo it


class Buffer:
    def __init__(self, port, script):
        self.c = Conn(port)
        self.script = script
        self.c.cmd("FLUSHALL")

    def add(self, spans, parent, root=False, nbytes=100, max_bytes=0, salt="salt", lock=False, rate=1):
        args = [len(spans), parent, "true" if root else "false", TIMEOUT, nbytes, max_bytes, salt,
                "true" if lock else "false", rate] + list(spans)
        reply = self.c.cmd("EVAL", self.script, 1, TRACE, *args)
        assert not isinstance(reply, Err), reply
        return reply

    def get(self, key):
        return self.c.cmd("GET", key)

    def ttl_ok(self, key):
        ttl = self.c.cmd("TTL", key)
        return 0 < ttl <= TIMEOUT


def table(flat):
    return dict(zip(flat[0::2], flat[1::2]))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--script", required=True)
    ap.add_argument("--port", type=int, default=6379)
    ap.add_argument("--junitxml")
    a = ap.parse_args()
    with open(a.script) as f:
        script = f.read()
    port = a.port
    checks = Checks()
    t0 = time.monotonic()

    def fresh():
        return Buffer(port, script)

    def reply_shape():
        b = fresh()
        r = b.add(["a", "b"], "p1")
        eq(len(r), 6, "reply length")
        eq(r[0], SPAN + "p1", "set_key")
        eq(r[1], None, "has_root_span (false is nil in a Lua reply)")
        assert isinstance(r[2], int) and r[2] >= 0, "latency_us %r" % (r[2],)
        assert isinstance(r[3], list) and isinstance(r[4], list) and isinstance(r[5], list), r
        eq(r[5], [], "merged_segment_span_ids")

    def sampled_tables():
        b = fresh()
        r = b.add(["a", "b"], "p1")
        lat, met = table(r[3]), table(r[4])
        eq(sorted(lat), sorted(["redirect_step_latency_us", "merge_payload_keys_step_latency_us",
                                "counter_merge_step_latency_us", "total_step_latency_us"]), "latency table keys")
        assert all(isinstance(v, int) and v >= 0 for v in lat.values()), lat
        eq(met, {"redirect_table_size": 0, "redirect_depth": 0, "detached_segment_too_large": 0,
                 "detached_segment_locked": 0}, "metrics table")
        eq(r[2], lat["total_step_latency_us"], "latency_us is the total step")

    def not_sampled():
        b = fresh()
        r = b.add(["a"], "p1", rate=NOT_SAMPLED)
        eq(r[2], -1, "latency_us when not sampled")
        eq(r[3], [], "latency_table when not sampled")
        eq(r[4], [], "metrics_table when not sampled")

    def first_subsegment_state():
        b = fresh()
        b.add(["a", "b"], "p1", nbytes=100, salt="s1")
        eq(sorted(b.c.cmd("HGETALL", SSR)), sorted(["a", "p1", "b", "p1"]), "redirect hash")
        eq(b.get(IC + "p1"), "2", "ingested count")
        eq(b.get(IBC + "p1"), "100", "ingested bytes")
        eq(b.c.cmd("SMEMBERS", MK + "p1"), ["s1"], "members")
        for key in (SSR, IC + "p1", IBC + "p1", MK + "p1"):
            assert b.ttl_ok(key), "TTL of %s: %r" % (key, b.c.cmd("TTL", key))
        eq(b.c.cmd("EXISTS", "span-buf:hrs:" + SPAN + "p1"), 0, "root flag key")

    def parent_arrives_and_absorbs_children():
        b = fresh()
        b.add(["a", "b"], "p1", nbytes=100, salt="s1")
        r = b.add(["p1"], "p0", nbytes=50, salt="s2")
        eq(r[0], SPAN + "p0", "set_key")
        eq(r[5], ["p1"], "merged_segment_span_ids")
        eq(b.get(IC + "p0"), "3", "ingested count of the merged segment")
        eq(b.get(IBC + "p0"), "150", "ingested bytes of the merged segment")
        eq(sorted(b.c.cmd("SMEMBERS", MK + "p0")), ["s1", "s2"], "members of the merged segment")
        eq(b.c.cmd("EXISTS", IC + "p1", IBC + "p1", MK + "p1"), 0, "the absorbed segment's keys")
        eq(b.c.cmd("HGET", SSR, "p1"), "p0", "redirect of the absorbed segment")

    def second_subsegment_follows_redirect():
        b = fresh()
        b.add(["a", "b"], "p1", salt="s1")
        b.add(["p1"], "p0", salt="s2")
        r = b.add(["d"], "p1", salt="s3")
        eq(r[0], SPAN + "p0", "set_key through the redirect")
        eq(table(r[4])["redirect_depth"], 1, "redirect_depth")
        eq(table(r[4])["redirect_table_size"], 3, "redirect_table_size")
        eq(b.get(IC + "p0"), "4", "ingested count")

    def root_flag():
        b = fresh()
        r = b.add(["a"], "p1", root=True)
        eq(r[1], 1, "has_root_span")
        eq(b.get("span-buf:hrs:" + SPAN + "p1"), "1", "root flag key")
        assert b.ttl_ok("span-buf:hrs:" + SPAN + "p1"), "TTL of the root flag"
        r = b.add(["b"], "p1", root=False)
        eq(r[1], 1, "has_root_span carried by the key")

    def over_byte_limit_detaches():
        b = fresh()
        b.add(["a"], "p1", nbytes=100, max_bytes=150, salt="s1")
        r = b.add(["b"], "p1", nbytes=100, max_bytes=150, salt="s2")
        eq(r[0], SPAN + "s2", "set_key of the detached subsegment")
        eq(table(r[4])["detached_segment_too_large"], 1, "detached_segment_too_large")
        eq(b.get(IC + "s2"), "1", "its own ingested count")
        eq(b.get(IC + "p1"), "1", "the first segment is unchanged")

    def flush_lock_detaches():
        b = fresh()
        b.add(["a"], "p1", salt="s1")
        b.c.cmd("SET", "span-buf:fl:" + SPAN + "p1", "1")
        r = b.add(["b"], "p1", salt="s2", lock=True)
        eq(r[0], SPAN + "s2", "set_key of the detached subsegment")
        eq(table(r[4])["detached_segment_locked"], 1, "detached_segment_locked")
        r = b.add(["c"], "p1", salt="s3", lock=False)
        eq(r[0], SPAN + "p1", "set_key when the lock is not checked")

    def merge_of_a_large_member_set():
        b = fresh()
        b.add(["a"], "p1", salt="s1")
        members = ["m%05d" % i for i in range(2500)]
        for i in range(0, len(members), 500):
            b.c.cmd("SADD", MK + "p1", *members[i:i + 500])
        r = b.add(["p1"], "p0", salt="s2")
        eq(r[5], ["p1"], "merged_segment_span_ids")
        eq(b.c.cmd("SCARD", MK + "p0"), 2502, "members after the merge (2,501 + the new salt)")
        eq(b.c.cmd("EXISTS", MK + "p1"), 0, "the absorbed set")

    def script_load_and_evalsha():
        b = fresh()
        sha = b.c.cmd("SCRIPT", "LOAD", script)
        eq(sha, hashlib.sha1(script.encode()).hexdigest(), "SCRIPT LOAD sha1")
        args = [1, "p1", "false", TIMEOUT, 10, 0, "s1", "false", NOT_SAMPLED, "a"]
        r = b.c.cmd("EVALSHA", sha, 1, TRACE, *args)
        assert not isinstance(r, Err), r
        eq(r[0], SPAN + "p1", "set_key by EVALSHA")
        eq(b.c.cmd("SCRIPT", "EXISTS", sha), [1], "SCRIPT EXISTS")

    def many_spans_in_one_call():
        b = fresh()
        ids = ["x%04d" % i for i in range(3000)]
        r = b.add(ids, "p1", nbytes=3000, salt="s1")
        eq(r[0], SPAN + "p1", "set_key")
        eq(b.c.cmd("HLEN", SSR), 3000, "redirect hash size")
        eq(b.get(IC + "p1"), "3000", "ingested count")

    def redirect_chain_of_depth_ten():
        b = fresh()
        for i in range(10):
            b.add(["s%d" % i], "s%d" % (i + 1), salt="k%d" % i)
        r = b.add(["new"], "s0", salt="kn")
        eq(r[0], SPAN + "s10", "set_key at the top of ten redirects")
        eq(table(r[4])["redirect_depth"], 10, "redirect_depth")

    for fn in (reply_shape, sampled_tables, not_sampled, first_subsegment_state,
               parent_arrives_and_absorbs_children, second_subsegment_follows_redirect, root_flag,
               over_byte_limit_detaches, flush_lock_detaches, merge_of_a_large_member_set,
               script_load_and_evalsha, many_spans_in_one_call, redirect_chain_of_depth_ten):
        checks.run("add-buffer: " + fn.__name__.replace("_", " "), fn)

    print("%d checks, %d failed" % (len(checks.rows), checks.failed()))
    if a.junitxml:
        write_junit(a.junitxml, "sentry-spans", checks.rows, time.monotonic() - t0)
    return 0


if __name__ == "__main__":
    sys.exit(main())
