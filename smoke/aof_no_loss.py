#!/usr/bin/env python3
"""Every acknowledged write must appear in the AOF.

The AOF queue used to be a bounded channel written with

    let _ = self.try_send(data);   // full -> record discarded

so once writes outran the AOF writer, records were dropped with no error, no
counter and no log line. The client still got +OK. A durability log that skips
entries under exactly the load you would enable it for is worse than no log,
because it still looks like it is working.

This drives writes hard enough to outrun the writer, then parses the AOF back
and checks that every acknowledged key is present. It is deliberately a
whole-file parse rather than a byte count: a torn or partially-written record
should fail here too.

Usage:  aof_no_loss.py --port 7999 --dir /path/containing/appendonly.aof [-n 200000]
Exit:   0 no loss | 1 records missing | 2 harness problem
"""

import argparse
import os
import socket
import sys
import time


def pipeline_sets(port, n, chunk=5000):
    """Send N SETs with as little round-tripping as possible, so the producer
    outruns the AOF writer. Returns the number the server acknowledged."""
    s = socket.create_connection(("127.0.0.1", port), 10)
    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    f = s.makefile("rb")
    acked = 0
    for start in range(0, n, chunk):
        end = min(start + chunk, n)
        buf = bytearray()
        for i in range(start, end):
            k = b"aofk:%d" % i
            v = b"v%d" % i
            buf += b"*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n" % (
                len(k),
                k,
                len(v),
                v,
            )
        s.sendall(buf)
        for _ in range(start, end):
            line = f.readline()
            if not line:
                raise IOError("connection closed after %d acks" % acked)
            if line[:1] != b"+":
                raise IOError("unexpected reply %r" % line)
            acked += 1
    s.close()
    return acked


def parse_aof_keys(path):
    """Parse the AOF as RESP commands; return (set_keys, other_command_count).

    Raises on a malformed record rather than skipping it -- a truncated tail is
    exactly the kind of corruption this test should catch.
    """
    with open(path, "rb") as fh:
        data = fh.read()

    keys = []
    others = 0
    pos = 0
    end = len(data)
    while pos < end:
        if data[pos : pos + 1] != b"*":
            raise ValueError("expected '*' at offset %d, found %r" % (pos, data[pos : pos + 1]))
        nl = data.index(b"\r\n", pos)
        argc = int(data[pos + 1 : nl])
        pos = nl + 2
        args = []
        for _ in range(argc):
            if data[pos : pos + 1] != b"$":
                raise ValueError("expected '$' at offset %d" % pos)
            nl = data.index(b"\r\n", pos)
            ln = int(data[pos + 1 : nl])
            pos = nl + 2
            args.append(data[pos : pos + ln])
            pos += ln + 2
        if args and args[0].upper() == b"SET":
            keys.append(args[1])
        else:
            others += 1
    return keys, others


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--dir", required=True, help="directory holding appendonly.aof")
    ap.add_argument("-n", type=int, default=200000)
    ap.add_argument("--drain-timeout", type=float, default=180.0,
                    help="seconds to wait for the writer to flush the queue")
    ap.add_argument("--label", default="")
    args = ap.parse_args()
    tag = (" [%s]" % args.label) if args.label else ""

    aof = os.path.join(args.dir, "appendonly.aof")

    # monotonic, not wall clock: a container or hosted runner whose clock
    # steps backward mid-run otherwise reports a negative duration (seen:
    # "acknowledged 200000 writes in -7.8s") and, worse, can stall the
    # drain poll below past its timeout.
    t0 = time.monotonic()
    acked = pipeline_sets(args.port, args.n)
    elapsed = time.monotonic() - t0
    print("ok    server acknowledged %d writes in %.1fs (%.0f ops/s)%s"
          % (acked, elapsed, acked / max(elapsed, 1e-9), tag))
    if acked != args.n:
        print("FAIL  only %d of %d acknowledged" % (acked, args.n))
        return 2

    # Wait for the writer to drain, rather than sleeping a fixed amount.
    #
    # This distinction is the whole point: a fixed sleep cannot tell "the record
    # was discarded" from "the record is still queued", and a 4s sleep here
    # originally reported a passing build as losing 63% of its writes. Poll
    # until the file stops growing, and report how long the drain took --
    # because a write that is acknowledged but not yet on disk is not durable
    # either, so the drain time is itself a result worth seeing.
    drain_start = time.monotonic()
    last, stable = -1, 0
    while time.monotonic() - drain_start < args.drain_timeout:
        size = os.path.getsize(aof) if os.path.exists(aof) else 0
        if size == last:
            stable += 1
            if stable >= 4:
                break
        else:
            stable = 0
        last = size
        time.sleep(0.5)
    drain = time.monotonic() - drain_start
    print("      drain         : %.1fs after the last ack" % drain)

    if not os.path.exists(aof):
        print("FAIL  no AOF at %s" % aof)
        return 2

    try:
        keys, others = parse_aof_keys(aof)
    except ValueError as e:
        print("FAIL  AOF is malformed: %s" % e)
        return 1

    want = {b"aofk:%d" % i for i in range(args.n)}
    got = set(keys)
    missing = want - got

    print("      AOF size      : %d bytes" % os.path.getsize(aof))
    print("      SET records   : %d (%d non-SET, e.g. SELECT)" % (len(keys), others))
    print("      distinct keys : %d of %d" % (len(got & want), args.n))

    if missing:
        pct = 100.0 * len(missing) / args.n
        sample = sorted(int(k.split(b":")[1]) for k in list(missing)[:5])
        print("\nFAIL  %d of %d acknowledged writes are absent from the AOF (%.2f%%)%s"
              % (len(missing), args.n, pct, tag))
        print("      e.g. keys %s" % sample)
        print("      These returned +OK to the client. On recovery they are gone.")
        return 1

    print("\nPASS  all %d acknowledged writes are present in the AOF%s" % (args.n, tag))
    return 0


if __name__ == "__main__":
    sys.exit(main())
