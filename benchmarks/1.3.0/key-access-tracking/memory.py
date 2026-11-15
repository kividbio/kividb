#!/usr/bin/env python3
"""Bytes per key with key-access-tracking off and on.

    memory.py --server target/release/kividb [--keys 10000000] [--json out.json]

For each mode a fresh server is started (io-model tokio, nothing persisted),
`--keys` small string keys ("k<i>" -> 8 bytes) are loaded in pipelines of
50,000, and `used_memory` and `used_memory_rss` are read from INFO; per key
is the growth over the empty server divided by DBSIZE. The access word is
part of every key's entry whatever the setting, so the two should agree to
the byte.
"""
import argparse
import json
import os
import socket
import subprocess
import tempfile
import time


def command(sock, *args):
    out = b"*%d\r\n" % len(args)
    for a in args:
        a = a if isinstance(a, bytes) else str(a).encode()
        out += b"$%d\r\n%s\r\n" % (len(a), a)
    sock.sendall(out)
    return read_reply(sock)


def read_reply(sock):
    buf = b""
    while not buf.endswith(b"\r\n"):
        buf += sock.recv(1)
    if buf[:1] == b"$":
        n = int(buf[1:-2])
        data = b""
        while len(data) < n + 2:
            data += sock.recv(n + 2 - len(data))
        return data[:-2]
    return buf[1:-2]


def info(sock, field):
    for line in command(sock, "INFO", "memory").decode().splitlines():
        if line.startswith(field + ":"):
            return int(line.split(":")[1])
    raise KeyError(field)


def measure(server, port, mode, keys):
    with tempfile.TemporaryDirectory() as directory:
        proc = subprocess.Popen(
            [server, "--port", str(port), "--bind", "127.0.0.1", "--io-model", "tokio",
             "--dir", directory, "--loglevel", "warning", "--key-access-tracking", mode],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            sock = None
            for _ in range(100):
                try:
                    sock = socket.create_connection(("127.0.0.1", port))
                    break
                except OSError:
                    time.sleep(0.1)
            assert sock, "server did not start"
            base = info(sock, "used_memory")
            base_rss = info(sock, "used_memory_rss")
            batch = 50_000
            for start in range(0, keys, batch):
                payload = b"".join(
                    b"*3\r\n$3\r\nSET\r\n$%d\r\nk%d\r\n$8\r\nvvvvvvvv\r\n" % (len(str(i)) + 1, i)
                    for i in range(start, min(start + batch, keys)))
                sock.sendall(payload)
                for _ in range(min(batch, keys - start)):
                    read_reply(sock)
            size = int(command(sock, "DBSIZE"))
            used = info(sock, "used_memory") - base
            rss = info(sock, "used_memory_rss") - base_rss
            return {"mode": mode, "keys": size, "used_memory_per_key": used / size,
                    "rss_per_key": rss / size}
        finally:
            proc.kill()
            proc.wait()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--server", required=True)
    ap.add_argument("--keys", type=int, default=10_000_000)
    ap.add_argument("--port", type=int, default=7100)
    ap.add_argument("--json")
    args = ap.parse_args()
    rows = [measure(args.server, args.port, mode, args.keys) for mode in ("no", "yes")]
    for row in rows:
        print(f"key-access-tracking {row['mode']:>3}: {row['keys']} keys, "
              f"{row['used_memory_per_key']:.2f} B/key used_memory, {row['rss_per_key']:.2f} B/key RSS")
    if args.json:
        json.dump(rows, open(args.json, "w"), indent=2)
