#!/usr/bin/env python3
"""r303fill.py <port> <prefix> <count>: RESP SETs of <prefix>:<n> with a 100-byte value, for redis-cli --pipe."""
import sys

_, _port, prefix, count = sys.argv
out = sys.stdout.buffer
for n in range(1, int(count) + 1):
    k = b"%s:%d" % (prefix.encode(), n)
    v = b"%0100d" % n
    out.write(b"*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$100\r\n%s\r\n" % (len(k), k, v))
