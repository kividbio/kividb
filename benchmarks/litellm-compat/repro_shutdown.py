"""What SHUTDOWN NOSAVE sends back before the server exits, raw and through redis-py."""
import socket
import sys

import redis


def raw(port, cmd):
    s = socket.create_connection(("127.0.0.1", port))
    parts = cmd.split()
    s.sendall(("*%d\r\n" % len(parts) + "".join("$%d\r\n%s\r\n" % (len(p), p) for p in parts)).encode())
    data = b""
    s.settimeout(3)
    while True:
        try:
            d = s.recv(200)
        except Exception as e:
            return data + repr(e).encode()
        if not d:
            return data + b"<closed>"
        data += d


mode, port = sys.argv[1], int(sys.argv[2])
if mode == "raw":
    print(port, sys.argv[3], "->", raw(port, sys.argv[3]))
else:
    try:
        redis.Redis(port=port).shutdown(nosave=True)
        print(port, "redis-py shutdown(nosave=True): no error")
    except Exception as e:
        print(port, "redis-py shutdown(nosave=True):", type(e).__name__, e)
