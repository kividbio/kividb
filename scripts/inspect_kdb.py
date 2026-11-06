#!/usr/bin/env python3
"""Parse a KIVIDB01 .kdb snapshot and report key counts, type histogram and payload sizes.

Read-only diagnostic used to characterise a snapshot before a memory repro.
"""
import struct
import sys
from collections import Counter, defaultdict

TAGS = {
    1: "string", 2: "list", 3: "hash", 4: "set", 5: "zset", 6: "stream",
    7: "json", 8: "timeseries", 9: "vectorset", 10: "hashex", 11: "array",
}


class R:
    def __init__(self, buf):
        self.b = buf
        self.i = 0

    def take(self, n):
        v = self.b[self.i:self.i + n]
        if len(v) != n:
            raise EOFError
        self.i += n
        return v

    def u32(self):
        return struct.unpack_from("<I", self.take(4))[0]

    def u64(self):
        return struct.unpack_from("<Q", self.take(8))[0]

    def i64(self):
        return struct.unpack_from("<q", self.take(8))[0]

    def u8(self):
        return self.take(1)[0]

    def blob(self):
        return self.take(self.u32())


def read_value(r, tag):
    """Returns (bytes_of_payload, element_count)."""
    start = r.i
    n = 0
    if tag == 1:
        r.blob()
    elif tag in (2, 4):
        n = r.u32()
        for _ in range(n):
            r.blob()
    elif tag == 3:
        n = r.u32()
        for _ in range(n):
            r.blob(); r.blob()
    elif tag == 5:
        n = r.u32()
        for _ in range(n):
            r.blob(); r.take(8)
    elif tag == 6:
        n = r.u32()
        for _ in range(n):
            r.take(16)
            f = r.u32()
            for _ in range(f):
                r.blob(); r.blob()
        r.take(16)
    elif tag == 7:
        r.blob()
    elif tag == 8:
        n = r.u32()
        r.take(16 * n)
        r.take(8); r.take(8)
        lc = r.u32()
        for _ in range(lc):
            r.blob(); r.blob()
    elif tag == 9:
        n = r.u32()
        for _ in range(n):
            r.blob()
            d = r.u32()
            r.take(4 * d)
            r.blob()
    elif tag == 10:
        n = r.u32()
        for _ in range(n):
            r.blob(); r.blob()
        ec = r.u32()
        for _ in range(ec):
            r.blob(); r.take(8)
    elif tag == 11:
        n = r.u32()
        for _ in range(n):
            r.take(8); r.blob()
        r.take(8); r.take(8)
    else:
        raise ValueError(f"unknown tag {tag}")
    return r.i - start, n


def main(path):
    with open(path, "rb") as f:
        buf = f.read()
    print(f"file: {path}  size={len(buf):,} bytes")
    r = R(buf)
    magic = r.take(8)
    print(f"magic: {magic!r}")
    if magic != b"KIVIDB01":
        print("not a native kdb snapshot")
        return
    db_count = r.u32()
    print(f"db_count: {db_count}")

    for db in range(db_count):
        types = Counter()
        payload = Counter()
        elems = Counter()
        keybytes = 0
        keys = 0
        while True:
            klen = r.u32()
            if klen == 0:
                break
            if klen == 0xFFFFFFFF:  # the empty key (#441): no bytes follow
                klen = 0
            r.take(klen)
            keybytes += klen
            keys += 1
            tag = r.u8()
            size, n = read_value(r, tag)
            name = TAGS.get(tag, f"tag{tag}")
            types[name] += 1
            payload[name] += size
            elems[name] += n
        if keys:
            print(f"\n--- db{db}: {keys:,} keys, key bytes={keybytes:,} ---")
            for name, c in types.most_common():
                print(f"  {name:<12} keys={c:<10,} payload={payload[name]:>15,}  elements={elems[name]:,}")

    rest = buf[r.i:]
    print(f"\ntrailing bytes after db data: {len(rest):,}")
    # The sections after the databases, in whatever order they were written
    # (the server reads each by its magic; since 1.2.1 the times to live and
    # the function libraries come before the index sections).
    sections = {
        b"KIVIEX1\x00": "times to live",
        b"KIVIFN1\x00": "function libraries",
        b"KIVIDX1\x00": "FT index schemas",
        b"KIVIHN1\x00": "persisted HNSW graphs",
        b"KIVIAL1\x00": "FT index aliases",
    }
    found = sorted((rest.find(magic), magic, what) for magic, what in sections.items() if magic in rest)
    for at, magic, what in found:
        name = magic[:7].decode()
        print(f"  contains {name} ({what}) at +{at:,}")
    if rest and not found:
        print(f"  head: {rest[:16]!r}")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "dump.kdb")
