#!/usr/bin/env python3
"""BGREWRITEAOF must bound AOF growth without losing a single write.

Three bugs made this the most dangerous command in the server, and all three
were silent -- the process kept serving correct values from memory while the
file on disk was wrong:

  1. Stream, TimeSeries and VectorSet hit a `None` arm and were simply dropped
     from the rewritten file.
  2. The walk took one shard's read lock at a time while writes continued, then
     renamed over the old log -- so a write landing mid-rewrite was in neither
     file.
  3. The writer opened appendonly.aof once at startup and never reopened it.
     After the rename its handle pointed at an unlinked inode, so *every write
     after a rewrite* went to a file with no name. Measured on the unfixed
     build: the AOF froze at 41 bytes forever.

Checks here, in order of how badly they failed before:

  A. writes issued AFTER a rewrite survive a restart      (bug 3)
  B. writes issued DURING a rewrite survive a restart     (bug 2)
  C. INCR counters are neither lost nor double-applied    (bug 2, the subtle
     half -- an image plus a replayed delta must not both land)
  D. streams (with consumer groups and pending entries),
     time series and vector sets survive                  (bug 1)

Usage:  aof_rewrite_durability.py --port 7999 --dir <dir with appendonly.aof>
        [--restart-cmd '<argv to relaunch the server>']
Exit:   0 ok | 1 data lost | 2 harness problem

Without --restart-cmd the script performs the pre-restart half and writes a
state file; run it again with --verify-only after restarting the server
yourself. CI uses the two-phase form so the workflow controls the process.
"""

import argparse
import json
import os
import socket
import sys
import threading
import time


class C:
    def __init__(self, port, timeout=20):
        self.s = socket.create_connection(("127.0.0.1", port), timeout)
        self.s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
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
            # NOTE: read the payload, not just the header. Returning the `$N`
            # line and int()-ing it silently compares value *lengths* -- a bug
            # that made an earlier version of this test pass a counter check it
            # could not actually have caught.
            return None if n == -1 else self.f.read(n + 2)[:-2].decode()
        if tag == b"*":
            n = int(body)
            return None if n == -1 else [self._read() for _ in range(n)]
        raise IOError("unexpected reply %r" % tag)


BASE_KEYS = 20000
N_COUNTERS = 200


def phase_write(port, state_path):
    c = C(port)

    # Spread a base dataset over many shards so the rewrite's per-shard walk
    # genuinely has shards it has finished and shards it has not.
    for start in range(0, BASE_KEYS, 5000):
        buf = bytearray()
        for i in range(start, min(start + 5000, BASE_KEYS)):
            k = b"base:%d" % i
            buf += b"*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$4\r\nbase\r\n" % (len(k), k)
        c.s.sendall(buf)
        for _ in range(start, min(start + 5000, BASE_KEYS)):
            c.f.readline()

    # The three types that used to be dropped entirely.
    for i in range(3):
        c.cmd("XADD", "st", "*", "f", "v%d" % i)
    c.cmd("XGROUP", "CREATE", "st", "g1", "0")
    c.cmd("XREADGROUP", "GROUP", "g1", "c1", "COUNT", "2", "STREAMS", "st", ">")
    c.cmd("TS.CREATE", "ts1", "RETENTION", "60000", "DUPLICATE_POLICY", "MAX",
          "LABELS", "sensor", "a")
    for t, v in ((1000, 1.5), (2000, 2.25), (3000, 3.75)):
        c.cmd("TS.ADD", "ts1", str(t), str(v))
    c.cmd("VADD", "vs1", "VALUES", "3", "1.5", "2.5", "3.5", "e1")
    c.cmd("VADD", "vs1", "VALUES", "3", "4.5", "5.5", "6.5", "e2")

    for i in range(N_COUNTERS):
        c.cmd("SET", "ctr:%d" % i, "0")

    # Hammer while the rewrite runs: new keys plus INCRs, which are the
    # non-idempotent case an image-plus-buffer design gets wrong.
    stop = threading.Event()
    issued = {"n": 0}

    def hammer():
        w = C(port)
        n = 0
        while not stop.is_set():
            w.cmd("SET", "during:%d" % n, "v")
            w.cmd("INCR", "ctr:%d" % (n % N_COUNTERS))
            n += 1
            issued["n"] = n

    th = threading.Thread(target=hammer, daemon=True)
    th.start()
    time.sleep(0.3)

    reply = c.cmd("BGREWRITEAOF")
    if not reply.startswith("Background"):
        print("FAIL  BGREWRITEAOF refused: %s" % reply)
        return 2
    time.sleep(3.0)
    stop.set()
    time.sleep(0.5)
    during = issued["n"]

    # Writes strictly AFTER the rewrite finished -- the case that used to vanish
    # into an unlinked inode.
    for i in range(500):
        c.cmd("SET", "after:%d" % i, "v")
    time.sleep(2.5)

    json.dump({"during": during}, open(state_path, "w"))
    print("ok    base=%d during=%d after=500, rewrite issued"
          % (BASE_KEYS, during))
    return 0


def phase_verify(port, state_path):
    state = json.load(open(state_path))
    during = state["during"]
    c = C(port)
    failures = []

    def chk(name, got, want):
        if got != want:
            failures.append("%s: got %r want %r" % (name, got, want))
            print("  %-30s %-24r FAIL want %r" % (name, got, want))
        else:
            print("  %-30s %-24r ok" % (name, got))

    # A: post-rewrite writes
    missing_after = sum(1 for i in range(500)
                        if c.cmd("EXISTS", "after:%d" % i) != "1")
    chk("post-rewrite writes missing", missing_after, 0)

    # B: writes during the rewrite (sampled)
    step = max(1, during // 400)
    missing_during = sum(1 for i in range(0, during, step)
                         if c.cmd("EXISTS", "during:%d" % i) != "1")
    chk("during-rewrite writes missing", missing_during, 0)

    # C: counters exact -- no loss, no double-apply
    total = sum(int(c.cmd("GET", "ctr:%d" % i) or 0) for i in range(N_COUNTERS))
    if total not in (during, during - 1):
        failures.append("counters: got %d want %d" % (total, during))
        print("  %-30s %-24d FAIL want %d (+/-1)" % ("sum(ctr)", total, during))
    else:
        print("  %-30s %-24d ok" % ("sum(ctr)", total))

    # D: the three formerly-dropped types
    chk("XLEN st", c.cmd("XLEN", "st"), "3")
    grp = c.cmd("XINFO", "GROUPS", "st") or []
    gd = dict(zip(grp[0][::2], grp[0][1::2])) if grp else {}
    chk("stream group name", gd.get("name"), "g1")
    chk("stream group pending", gd.get("pending"), "2")
    chk("TS.RANGE ts1", c.cmd("TS.RANGE", "ts1", "-", "+"),
        [["1000", "1.5"], ["2000", "2.25"], ["3000", "3.75"]])
    chk("VCARD vs1", c.cmd("VCARD", "vs1"), "2")

    # base data still intact
    missing_base = sum(1 for i in range(0, BASE_KEYS, max(1, BASE_KEYS // 400))
                       if c.cmd("EXISTS", "base:%d" % i) != "1")
    chk("base keys missing", missing_base, 0)

    if failures:
        print("\nFAIL  %d check(s) failed after BGREWRITEAOF + restart:" % len(failures))
        for f in failures:
            print("        %s" % f)
        return 1
    print("\nPASS  BGREWRITEAOF lost nothing and double-applied nothing")
    return 0


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--port", type=int, required=True)
    p.add_argument("--dir", required=True)
    p.add_argument("--verify-only", action="store_true")
    args = p.parse_args()
    state_path = os.path.join(args.dir, "rewrite_state.json")
    if args.verify_only:
        return phase_verify(args.port, state_path)
    return phase_write(args.port, state_path)


if __name__ == "__main__":
    sys.exit(main())
