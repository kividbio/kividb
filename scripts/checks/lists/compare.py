#!/usr/bin/env python3
"""compare.py <redis-port> <kividb-port>: one sequence of list commands against both,
replies compared exactly (a list's order is part of its value). After every write the
whole list is read back (LLEN, LRANGE 0 -1) and compared. OBJECT ENCODING is compared
on its own: KiviDB says `listpack` while a list is one chunk (64 elements at most, none
over 1,024 bytes), Redis while it fits list-max-listpack-size (8 kB by default), so
those differences are listed apart. Prints every difference and a summary line."""
import socket, sys


class C:
    def __init__(self, port):
        self.s = socket.create_connection(("127.0.0.1", port))
        self.f = self.s.makefile("rb")

    def cmd(self, *args):
        args = [a if isinstance(a, bytes) else str(a).encode() for a in args]
        out = b"*%d\r\n" % len(args) + b"".join(b"$%d\r\n%s\r\n" % (len(a), a) for a in args)
        self.s.sendall(out)
        return self.read()

    def read(self):
        line = self.f.readline()
        t, rest = line[:1], line[1:-2]
        if t == b"+":
            return rest.decode()
        if t == b"-":
            return ("ERR", rest.decode().split(" ")[0])
        if t == b":":
            return int(rest)
        if t == b"$":
            n = int(rest)
            return None if n < 0 else self.f.read(n + 2)[:-2]
        if t in (b"*", b"~"):
            n = int(rest)
            return None if n < 0 else [self.read() for _ in range(n)]
        if t == b"_":
            return None
        raise RuntimeError(line)


r, k = C(int(sys.argv[1])), C(int(sys.argv[2]))
diffs, enc_diffs, total = [], [], 0


def both(*args, kind="exact"):
    global total
    total += 1
    a, b = r.cmd(*args), k.cmd(*args)
    if kind == "exact":
        ok = a == b
    elif kind == "shape":  # same type
        ok = type(a) == type(b)
    if not ok:
        diffs.append((args, a, b))
    return a, b


def enc(key, step=""):
    a, b = r.cmd("OBJECT", "ENCODING", key), k.cmd("OBJECT", "ENCODING", key)
    if a != b:
        enc_diffs.append((key, step, r.cmd("LLEN", key), a, b))
    return b


def state(key, step=""):
    both("LLEN", key)
    both("LRANGE", key, 0, -1)
    both("EXISTS", key)
    enc(key, step)


def reads(key):
    """Every read of a list."""
    n = r.cmd("LLEN", key) or 0
    for i in (0, 1, n // 2, n - 1, n, -1, -2, -(n // 2), -n, -n - 1, 63, 64, 65, 32):
        both("LINDEX", key, i)
    for a, b in ((0, 9), (0, 99), (-100, -1), (5, 3), (n - 3, n + 10), (-5, 2), (60, 70), (0, -1), (1, -2)):
        both("LRANGE", key, a, b)
    first = r.cmd("LINDEX", key, 0)
    mid = r.cmd("LINDEX", key, n // 2)
    for el in (first, mid, b"absent", b"x"):
        if el is None:
            continue
        both("LPOS", key, el)
        both("LPOS", key, el, "RANK", -1)
        both("LPOS", key, el, "RANK", 2)
        both("LPOS", key, el, "COUNT", 0)
        both("LPOS", key, el, "COUNT", 3, "RANK", -2)
        both("LPOS", key, el, "COUNT", 0, "MAXLEN", 10)
        both("LPOS", key, el, "RANK", -1, "MAXLEN", 10)
    both("MEMORY", "USAGE", key, kind="shape")
    both("TYPE", key)
    state(key, "read")


def el(i, p="element"):
    return "%s:%024d" % (p, i)  # 32 bytes


def push(key, items, cmd="RPUSH"):
    for i in range(0, len(items), 100):
        both(cmd, key, *items[i:i + 100])


LONG = "L" * 2000
LONGEST = "l" * 1024
JUST_OVER = "o" * 1025

for c in (r, k):
    c.cmd("FLUSHALL")

# Shapes: one chunk, many chunks (regular), irregular, plain (a long element in it),
# plain after the long element went, and odd elements (empty, numbers, the longest).
push("one", [el(i) for i in range(10)])
push("many", [el(i) for i in range(300)])
push("irregular", [el(i) for i in range(300)])
both("LINSERT", "irregular", "BEFORE", el(150), "inserted")
both("LINSERT", "irregular", "AFTER", el(20), "inserted again")
push("plain", [el(i) for i in range(100)] + [LONG] + [el(i) for i in range(100, 150)])
push("plain-after", [LONG] + [el(i) for i in range(80)])
both("LPOP", "plain-after")
push("odd", ["", "1", "-7", "3.5", LONGEST, "x", "x", "", "x"])
# One chunk of large elements: 20 of 1,000 bytes, past Redis's 8 kB node.
push("kb", ["%04d" % i + "k" * 996 for i in range(20)])
SHAPES = ["one", "many", "irregular", "plain", "plain-after", "odd", "kb"]
for key in SHAPES:
    reads(key)

# Growth an element at a time across the chunk boundary, from both ends; and in batches.
for i in range(70):
    both("RPUSH", "grow-r", el(i))
    enc("grow-r", "rpush %d" % (i + 1))
    both("LPUSH", "grow-l", el(i))
    enc("grow-l", "lpush %d" % (i + 1))
state("grow-r")
state("grow-l")
both("LPUSH", "batch-l", *[el(i) for i in range(150)])
both("RPUSH", "batch-r", *[el(i) for i in range(150)])
both("LPUSHX", "batch-l", *[el(i, "x") for i in range(70)])
both("RPUSHX", "batch-r", *[el(i, "x") for i in range(70)])
both("LPUSHX", "nokey", "a", "b")
both("RPUSHX", "nokey", "a")
state("batch-l")
state("batch-r")
reads("batch-l")
# Shrinking an element at a time back to one chunk and to nothing.
for i in range(75):
    both("RPOP", "grow-r")
    enc("grow-r", "rpop")
    both("LPOP", "grow-l")
    enc("grow-l", "lpop")
state("grow-r")
state("grow-l")

# Every write on every shape, the whole list compared after each.
for key in SHAPES:
    n = r.cmd("LLEN", key)
    both("LSET", key, 0, "set-first")
    both("LSET", key, -1, "set-last")
    both("LSET", key, n // 2, "set-middle, a longer element than the one there")
    both("LSET", key, 33, "s")
    both("LSET", key, n, "out of range")
    both("LSET", key, -n - 1, "out of range")
    state(key, "lset")
    both("LINSERT", key, "BEFORE", "set-middle, a longer element than the one there", "before-mid")
    both("LINSERT", key, "AFTER", "set-last", "after-last")
    both("LINSERT", key, "BEFORE", "set-first", "before-first")
    both("LINSERT", key, "AFTER", "absent", "never")
    both("LINSERT", key, "SIDEWAYS", "x", "y")
    state(key, "linsert")
    both("RPUSH", key, "x", "y", "x", "x", "z")
    both("LPUSH", key, "x", "w")
    both("LREM", key, 1, "x")
    both("LREM", key, -1, "x")
    both("LREM", key, 0, "x")
    both("LREM", key, 0, "absent")
    both("LREM", key, 2, el(3))
    state(key, "lrem")
    both("LPOP", key)
    both("RPOP", key)
    both("LPOP", key, 3)
    both("RPOP", key, 2)
    both("LPOP", key, 0)
    state(key, "pop")
    both("LMPOP", 2, "absent", key, "LEFT", "COUNT", 2)
    both("LMPOP", 1, key, "RIGHT")
    both("BLPOP", key, 0.01)
    both("BRPOP", "absent", key, 0.01)
    both("BLMPOP", 0.01, 1, key, "RIGHT", "COUNT", 2)
    state(key, "mpop")
    both("LMOVE", key, key, "LEFT", "RIGHT")
    both("LMOVE", key, key, "RIGHT", "LEFT")
    both("RPOPLPUSH", key, key)
    both("LMOVE", key, key + "-dst", "LEFT", "LEFT")
    both("LMOVE", key, key + "-dst", "RIGHT", "RIGHT")
    both("BLMOVE", key, key + "-dst", "LEFT", "RIGHT", 0.01)
    both("BRPOPLPUSH", key, key + "-dst", 0.01)
    state(key, "move")
    state(key + "-dst", "move dst")
    reads(key)
    both("LTRIM", key, 1, -2)
    state(key, "ltrim")
    both("LTRIM", key, 2, 100)
    state(key, "ltrim")
    both("LTRIM", key, -50, -3)
    state(key, "ltrim")
    reads(key)

# A move between a packed list and a plain one, both ways; and onto the wrong type.
push("p-src", [el(i) for i in range(20)])
push("q-dst", [LONG, "a", "b"])
both("LMOVE", "p-src", "q-dst", "LEFT", "LEFT")
both("LMOVE", "q-dst", "p-src", "RIGHT", "LEFT")
both("LMOVE", "q-dst", "p-src", "LEFT", "LEFT")  # the long element into the packed list
state("p-src")
state("q-dst")
both("SET", "str", "v")
both("LMOVE", "p-src", "str", "LEFT", "LEFT")
both("RPOPLPUSH", "str", "p-src")
both("LPUSH", "str", "v")
state("p-src")
# The source emptied by a move is deleted.
both("RPUSH", "last", "only")
both("LMOVE", "last", "newdst", "LEFT", "LEFT")
both("EXISTS", "last")
state("newdst")

# Conversions by a long element: pushed, set, inserted; the longest that still packs.
for how in ("rpush", "lpush", "lset", "linsert", "longest"):
    key = "conv-" + how
    push(key, [el(i) for i in range(100)])
    if how == "rpush":
        both("RPUSH", key, JUST_OVER)
    elif how == "lpush":
        both("LPUSH", key, JUST_OVER)
    elif how == "lset":
        both("LSET", key, 40, JUST_OVER)
    elif how == "linsert":
        both("LINSERT", key, "AFTER", el(70), JUST_OVER)
    else:
        both("RPUSH", key, LONGEST)
    state(key, how)
    both("LREM", key, 0, JUST_OVER)
    state(key, how + " removed")
    reads(key)

# Emptied lists are deleted, whatever the shape; a new one starts again.
for key in ("one", "plain"):
    both("LTRIM", key, 1, 0)
    both("EXISTS", key)
    both("RPUSH", key, "again")
    state(key, "again")
both("RPUSH", "drain", *[el(i) for i in range(130)])
both("LPOP", "drain", 200)
both("EXISTS", "drain")
both("RPUSH", "drain2", "a", "x", "x")
both("LREM", "drain2", 0, "x")
both("LREM", "drain2", 0, "a")
both("EXISTS", "drain2")

# SORT of each shape; COPY, RENAME, DEL, UNLINK.
push("nums", [str((i * 7919) % 1000) for i in range(200)])
for key in ("nums", "many", "odd", "plain"):
    both("SORT", key, "ALPHA")
    both("SORT", key, "ALPHA", "DESC", "LIMIT", 5, 10)
both("SORT", "nums")
both("SORT", "nums", "STORE", "sorted")
state("sorted")
both("SORT", "odd", "ALPHA", "STORE", "sorted-odd")
state("sorted-odd")
for key in ("many", "irregular", "plain", "plain-after"):
    both("COPY", key, key + "-copy")
    state(key + "-copy")
    both("RPUSH", key + "-copy", "only in the copy")
    state(key)
    both("RENAME", key + "-copy", key + "-renamed")
    state(key + "-renamed")
    both("DEL", key + "-renamed")
    both("UNLINK", key)
    both("EXISTS", key)
both("DEL", "absent")
both("RPUSH", "str", "x")
both("LLEN", "str")
both("LRANGE", "str", 0, -1)

# The capped list and the queue, a while.
for i in range(3000):
    both("LPUSH", "capped", el(i))
    both("LTRIM", "capped", 0, 99)
state("capped")
for i in range(3000):
    both("RPUSH", "queue", el(i))
    if i >= 50:
        both("LPOP", "queue")
state("queue")
reads("capped")

print("%d commands, %d differences, %d encoding differences" % (total, len(diffs), len(enc_diffs)))
for args, a, b in diffs[:60]:
    show = lambda x: (repr(x)[:200])  # noqa: E731
    print("DIFF", " ".join(str(x)[:40] for x in args), "| redis:", show(a), "| kividb:", show(b))
# Encoding differences, grouped: key, step, length, redis, kividb.
seen = {}
for key, step, n, a, b in enc_diffs:
    seen.setdefault((a, b), []).append("%s/%s@%s" % (key, step, n))
for (a, b), where in seen.items():
    print("ENCODING redis=%s kividb=%s (%d times): %s" % (a, b, len(where), " ".join(where[:12])))
