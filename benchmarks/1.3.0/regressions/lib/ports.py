#!/usr/bin/env python3
"""Print a free TCP port on 127.0.0.1 outside the ephemeral range.

A port from the ephemeral range (Linux: net.ipv4.ip_local_port_range,
usually 32768-60999) can be taken by a client's own outgoing connection
between our check and the server's bind, and a client retrying against a
server that is not up yet can then connect to itself. So pick from below the
range, at random, and check that the port can be bound and that nothing
listens on it.
"""
import random
import socket


def ephemeral_range():
    try:
        with open("/proc/sys/net/ipv4/ip_local_port_range") as f:
            lo, hi = (int(x) for x in f.read().split())
            return lo, hi
    except OSError:
        return 32768, 60999


def free_port(lo=20000, hi=30000, tries=200):
    elo, ehi = ephemeral_range()
    rng = random.SystemRandom()
    for _ in range(tries):
        port = rng.randrange(lo, hi)
        if elo <= port <= ehi:
            continue
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            if s.connect_ex(("127.0.0.1", port)) == 0:
                continue
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.bind(("127.0.0.1", port))
            except OSError:
                continue
        return port
    raise SystemExit("no free port found below the ephemeral range")


if __name__ == "__main__":
    print(free_port())
