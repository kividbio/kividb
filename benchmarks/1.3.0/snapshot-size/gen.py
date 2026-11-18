#!/usr/bin/env python3
"""RESP generator for the #879 datasets, for `redis-cli --pipe`. Fixed seeds.

  gen.py rand6m   6M strings key:<n>, random bytes 16-64
  gen.py text6m   6M strings key:<n>, text-like 16-64 (words, ids, JSON-ish)
  gen.py coll     1M hashes x10 fields, 200k sets x20 ints, 200k zsets x20, 200k lists x20
  gen.py vec      200k hashes with a 128-dim FLOAT32 vector and a tag (FT.CREATE sent by bench)
"""
import os
import random
import struct
import sys

out = sys.stdout.buffer


def cmd(*args):
    parts = [b"*%d\r\n" % len(args)]
    for a in args:
        if isinstance(a, str):
            a = a.encode()
        elif isinstance(a, int):
            a = b"%d" % a
        parts.append(b"$%d\r\n%s\r\n" % (len(a), a))
    return b"".join(parts)


WORDS = None


def words():
    global WORDS
    if WORDS is None:
        rnd = random.Random(7)
        syl = ["ka", "lo", "mi", "ne", "ra", "to", "su", "vi", "an", "el", "or", "us",
               "ber", "dan", "fel", "gor", "hin", "jat", "kel", "mor", "pex", "quo"]
        WORDS = sorted({"".join(rnd.choice(syl) for _ in range(rnd.randint(1, 3)))
                        for _ in range(3000)})[:1000]
    return WORDS


def text_value(rnd, lo, hi):
    n = rnd.randint(lo, hi)
    kind = rnd.random()
    w = words()
    if kind < 0.4:
        s = " ".join(rnd.choice(w) for _ in range(12))
    elif kind < 0.7:
        s = '{"id":%d,"u":"%s","st":"%s","n":%d}' % (
            rnd.randint(1, 10_000_000), rnd.choice(w), rnd.choice(["active", "idle", "gone"]),
            rnd.randint(0, 999))
    else:
        s = "user:%d:%s:%s" % (rnd.randint(1, 10_000_000), rnd.choice(w), rnd.choice(w))
    s = (s * 4)[:n]
    return s


def flush(buf):
    out.write(b"".join(buf))
    buf.clear()


def main():
    what = sys.argv[1]
    rnd = random.Random(879)
    buf = []
    if what in ("rand6m", "text6m"):
        for i in range(6_000_000):
            if what == "rand6m":
                v = os.urandom(rnd.randint(16, 64)) if False else rnd.randbytes(rnd.randint(16, 64))
            else:
                v = text_value(rnd, 16, 64)
            buf.append(cmd("SET", "key:%d" % i, v))
            if len(buf) >= 10000:
                flush(buf)
    elif what == "coll":
        w = words()
        for i in range(1_000_000):
            args = ["HSET", "user:%d" % i]
            for f in range(10):
                fname = ["name", "email", "city", "age", "score", "plan", "created", "flags",
                         "last_ip", "bio"][f]
                if f in (3, 4, 6):
                    v = str(rnd.randint(0, 2_000_000_000 if f == 6 else 1000))
                else:
                    v = text_value(rnd, 6, 24)
                args += [fname, v]
            buf.append(cmd(*args))
            if len(buf) >= 2000:
                flush(buf)
        for i in range(200_000):
            buf.append(cmd("SADD", "tags:%d" % i, *[str(rnd.randint(0, 100000)) for _ in range(20)]))
            zargs = ["ZADD", "board:%d" % i]
            for j in range(20):
                zargs += [str(round(rnd.random() * 1000, 2)), rnd.choice(w) + str(j)]
            buf.append(cmd(*zargs))
            buf.append(cmd("RPUSH", "q:%d" % i, *[text_value(rnd, 8, 32) for _ in range(20)]))
            if len(buf) >= 3000:
                flush(buf)
    elif what == "vec":
        for i in range(200_000):
            vec = struct.pack("<128f", *[rnd.gauss(0, 1) for _ in range(128)])
            buf.append(cmd("HSET", "doc:%d" % i, "v", vec, "tag", rnd.choice(["a", "b", "c", "d"]),
                           "title", text_value(rnd, 16, 48)))
            if len(buf) >= 2000:
                flush(buf)
    else:
        sys.exit("unknown dataset")
    flush(buf)


main()
