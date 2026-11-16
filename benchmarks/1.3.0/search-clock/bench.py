#!/usr/bin/env python3
"""Raw-RESP client for run.sh: load an index of hashes, or run search queries.

  bench.py load <port> <docs> <ttl 0|1>
  bench.py run  <port> <queries> <kind>

Kinds: search-knn, search-filter-knn, search-tag, aggregate-knn,
aggregate-filter-knn. No dependencies beyond the standard library.
"""
import random
import socket
import struct
import sys

DIM = 8


def enc(*parts):
    out = [b"*%d\r\n" % len(parts)]
    for p in parts:
        if isinstance(p, str):
            p = p.encode()
        elif isinstance(p, int):
            p = str(p).encode()
        out.append(b"$%d\r\n%s\r\n" % (len(p), p))
    return b"".join(out)


class Conn:
    def __init__(self, port):
        self.s = socket.create_connection(("127.0.0.1", port))
        self.buf = b""

    def _line(self):
        while b"\r\n" not in self.buf:
            self.buf += self.s.recv(1 << 16)
        line, self.buf = self.buf.split(b"\r\n", 1)
        return line

    def _bytes(self, n):
        while len(self.buf) < n + 2:
            self.buf += self.s.recv(1 << 16)
        data, self.buf = self.buf[:n], self.buf[n + 2:]
        return data

    def read(self):
        line = self._line()
        kind, rest = line[:1], line[1:]
        if kind == b"*":
            n = int(rest)
            return [self.read() for _ in range(max(n, 0))]
        if kind == b"$":
            n = int(rest)
            return None if n < 0 else self._bytes(n)
        if kind == b"-":
            raise RuntimeError(rest.decode())
        return rest

    def cmd(self, *parts):
        self.s.sendall(enc(*parts))
        return self.read()

    def pipeline(self, cmds):
        self.s.sendall(b"".join(enc(*c) for c in cmds))
        return [self.read() for _ in cmds]


def vec(rng):
    return struct.pack("<%df" % DIM, *[rng.random() for _ in range(DIM)])


def load(port, docs, ttl):
    c = Conn(port)
    c.cmd("FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "d:", "SCHEMA",
          "t", "TAG", "n", "NUMERIC",
          "vec", "VECTOR", "FLAT", "6", "TYPE", "FLOAT32", "DIM", str(DIM),
          "DISTANCE_METRIC", "L2")
    rng = random.Random(670)
    tags = ["red", "green", "blue"]
    batch = []
    for i in range(docs):
        batch.append(["HSET", f"d:{i}", "t", tags[i % 3], "n", str(i),
                      "x", "extra", "vec", vec(rng)])
        if ttl:
            # a far deadline: the hash is a HashEx, and nothing expires
            batch.append(["HPEXPIRE", f"d:{i}", "36000000", "FIELDS", "1", "x"])
        if len(batch) >= 200:
            c.pipeline(batch)
            batch = []
    if batch:
        c.pipeline(batch)
    print("loaded", docs, "ttl", ttl)


def query(kind, q):
    p = ["PARAMS", "2", "q", q, "DIALECT", "2"]
    if kind == "search-knn":
        return ["FT.SEARCH", "idx", "*=>[KNN 10 @vec $q]"] + p
    if kind == "search-filter-knn":
        return ["FT.SEARCH", "idx", "(@t:{red})=>[KNN 10 @vec $q]"] + p
    if kind == "search-tag":
        return ["FT.SEARCH", "idx", "@t:{red}", "LIMIT", "0", "10"]
    if kind == "aggregate-knn":
        return ["FT.AGGREGATE", "idx", "*=>[KNN 10 @vec $q AS d]"] + p
    if kind == "aggregate-filter-knn":
        return ["FT.AGGREGATE", "idx", "(@t:{red})=>[KNN 10 @vec $q AS d]"] + p
    raise SystemExit("unknown kind " + kind)


def run(port, queries, kind):
    c = Conn(port)
    rng = random.Random(7)
    for _ in range(queries):
        reply = c.cmd(*query(kind, vec(rng)))
        assert reply and reply[0] != 0, reply


if __name__ == "__main__":
    mode, port = sys.argv[1], int(sys.argv[2])
    if mode == "load":
        load(port, int(sys.argv[3]), int(sys.argv[4]))
    else:
        run(port, int(sys.argv[3]), sys.argv[4])
