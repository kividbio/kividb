#!/usr/bin/env python3
"""A replica must converge with its master on non-deterministic writes.

kividb propagated the client's command verbatim, so commands whose result is
chosen at execution time were re-decided independently on each side:

    SPOP      picks random members     -> replica removed a different subset
    XADD *    mints an id from now()   -> replica assigned different ids

Measured on the unfixed build: the replica kept a different 12 of 20 set
members, and 4 of 5 stream ids differed by a millisecond. Both are silent --
no error anywhere, the replica simply holds different data. That matters most
on failover, where exactly this replica is promoted.

The fix propagates the command's *effect* (SPOP -> SREM of the members actually
popped, XADD * -> XADD with the assigned id); see
src/replication/script_feed.rs.

SADD is the control. It is deterministic, so it must converge on any build. If
the control fails, replication itself is broken and the rest is meaningless --
that is reported separately rather than as a determinism failure.

Usage:  replication_determinism.py --master-port 7801 --replica-port 7802
Exit:   0 all converged | 1 divergence | 2 harness/replication broken
"""

import argparse
import socket
import sys
import time


class Conn:
    def __init__(self, port):
        self.s = socket.create_connection(("127.0.0.1", port), 5)
        self.f = self.s.makefile("rb")

    def cmd(self, *args):
        out = b"*%d\r\n" % len(args)
        for a in args:
            b = a.encode() if isinstance(a, str) else a
            out += b"$%d\r\n%s\r\n" % (len(b), b)
        self.s.sendall(out)
        return self._read()

    def _read(self):
        line = self.f.readline()
        if not line:
            raise IOError("connection closed")
        tag, body = line[:1], line[1:].strip()
        if tag in b"+:":
            return body.decode()
        if tag == b"-":
            return "ERR " + body.decode()
        if tag == b"$":
            n = int(body)
            return None if n == -1 else self.f.read(n + 2)[:-2].decode()
        if tag == b"*":
            n = int(body)
            return None if n == -1 else [self._read() for _ in range(n)]
        raise IOError("unexpected reply type %r" % tag)


def wait_linked(replica, timeout=30):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if "master_link_status:up" in replica.cmd("INFO", "replication"):
            return True
        time.sleep(0.3)
    return False


def settle(view, timeout=10):
    """Poll the replica until its view stops changing, so replication lag is
    never mistaken for divergence."""
    deadline = time.monotonic() + timeout
    last = object()
    while time.monotonic() < deadline:
        cur = view()
        if cur == last:
            return cur
        last = cur
        time.sleep(0.4)
    return last


class Report:
    def __init__(self):
        self.rows = []

    def check(self, name, master, replica, note=""):
        ok = master == replica
        self.rows.append((name, ok))
        print("\n--- %s ---" % name)
        print("  master : %s" % (master,))
        print("  replica: %s" % (replica,))
        if note:
            print("  %s" % note)
        print("  => %s" % ("agree" if ok else "DIVERGED"))
        return ok


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--master-port", type=int, required=True)
    p.add_argument("--replica-port", type=int, required=True)
    p.add_argument("--label", default="")
    args = p.parse_args()

    tag = (" [%s]" % args.label) if args.label else ""
    m, r = Conn(args.master_port), Conn(args.replica_port)

    if not wait_linked(r):
        print("FAIL  replica never linked to master%s" % tag)
        print(r.cmd("INFO", "replication"))
        return 2
    print("ok    replica linked to master%s" % tag)

    rep = Report()
    members = ["m%02d" % i for i in range(20)]

    # ---- control -----------------------------------------------------------
    m.cmd("DEL", "det:ctl")
    m.cmd("SADD", "det:ctl", *members)
    mv = sorted(m.cmd("SMEMBERS", "det:ctl") or [])
    rv = settle(lambda: sorted(r.cmd("SMEMBERS", "det:ctl") or []))
    if not rep.check("control: SADD is deterministic", mv, rv):
        print("\nFAIL  the control diverged, so replication itself is broken.")
        print("      Fix that first -- the probes below prove nothing until it")
        print("      passes.")
        return 2

    # ---- SPOP --------------------------------------------------------------
    m.cmd("DEL", "det:set")
    m.cmd("SADD", "det:set", *members)
    settle(lambda: sorted(r.cmd("SMEMBERS", "det:set") or []))
    popped = sorted(m.cmd("SPOP", "det:set", "8") or [])
    mv = sorted(m.cmd("SMEMBERS", "det:set") or [])
    rv = settle(lambda: sorted(r.cmd("SMEMBERS", "det:set") or []))
    rep.check(
        "SPOP key 8 (random selection)",
        mv,
        rv,
        "master popped %s; equal cardinality (%s) with different members is the"
        " verbatim-propagation signature" % (popped, len(mv) == len(rv)),
    )

    # ---- XADD * ------------------------------------------------------------
    m.cmd("DEL", "det:stream")
    for i in range(5):
        m.cmd("XADD", "det:stream", "*", "f", "v%d" % i)
        time.sleep(0.05)
    mv = [e[0] for e in (m.cmd("XRANGE", "det:stream", "-", "+") or [])]
    rv = settle(lambda: [e[0] for e in (r.cmd("XRANGE", "det:stream", "-", "+") or [])])
    rep.check("XADD key * (server-generated id)", mv, rv, "entry ids")

    # ---- XADD * behind options (the rewrite must find the right argument) ---
    m.cmd("DEL", "det:stream2")
    for i in range(5):
        m.cmd("XADD", "det:stream2", "MAXLEN", "~", "100", "*", "f", "v%d" % i)
        time.sleep(0.05)
    mv = [e[0] for e in (m.cmd("XRANGE", "det:stream2", "-", "+") or [])]
    rv = settle(
        lambda: [e[0] for e in (r.cmd("XRANGE", "det:stream2", "-", "+") or [])]
    )
    rep.check("XADD key MAXLEN ~ 100 * (id after options)", mv, rv, "entry ids")

    # ---- INCRBYFLOAT -------------------------------------------------------
    m.cmd("DEL", "det:float")
    m.cmd("SET", "det:float", "1")
    for _ in range(5):
        m.cmd("INCRBYFLOAT", "det:float", "1.1")
    mv = m.cmd("GET", "det:float")
    rv = settle(lambda: r.cmd("GET", "det:float"))
    rep.check("INCRBYFLOAT (non-idempotent delta)", mv, rv)

    # ---- summary -----------------------------------------------------------
    print("\n" + "=" * 64)
    for name, ok in rep.rows:
        print("  %-48s %s" % (name, "agree" if ok else "DIVERGED"))
    bad = [n for n, ok in rep.rows if not ok]
    if bad:
        print("\n%d of %d diverged%s." % (len(bad), len(rep.rows), tag))
        return 1
    print("\nAll %d checks converged%s." % (len(rep.rows), tag))
    return 0


if __name__ == "__main__":
    sys.exit(main())
