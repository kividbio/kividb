"""Raw RESP3 replies to Lua scripts that return tables, on two servers. usage: repro_resp3_lua.py PORT [PORT...]"""
import socket
import sys
import time


def enc(*parts):
    out = b"*%d\r\n" % len(parts)
    for p in parts:
        p = p if isinstance(p, bytes) else str(p).encode()
        out += b"$%d\r\n%s\r\n" % (len(p), p)
    return out


def ask(s, *parts):
    s.sendall(enc(*parts))
    time.sleep(0.15)
    s.settimeout(0.5)
    data = b""
    try:
        while True:
            d = s.recv(65536)
            if not d:
                break
            data += d
    except socket.timeout:
        pass
    return data


SCRIPTS = [
    "return {1, false, '3', 0}",
    "return {1, redis.call('GET', KEYS[1]), '3', 0}",
    "return redis.call('GET', KEYS[1])",
    "return false",
    "return {false}",
    "return {1, 2, 3}",
    "return {'a', false, 'b'}",
    "return redis.call('ZCOUNT', KEYS[1], '-inf', '+inf')",
    "return {redis.call('ZCOUNT', KEYS[1], '-inf', '+inf'), redis.call('GET', KEYS[1]), redis.call('GET', KEYS[1]), 0}",
    "return redis.call('SET', KEYS[1], 'x', 'NX', 'EX', 10)",
]

for port in map(int, sys.argv[1:]):
    s = socket.create_connection(("127.0.0.1", port))
    hello = ask(s, "HELLO", "3")
    ver = [l for l in hello.split(b"\r\n") if l.startswith(b"8.") or l.startswith(b"7.")][:1]
    print(f"== port {port} HELLO 3 -> version {ver}")
    ask(s, "DEL", "missing", "setkey")
    for sc in SCRIPTS:
        key = "setkey" if "SET'" in sc else "missing"
        print(f"  EVAL {sc!r} 1 {key}\n    -> {ask(s, 'EVAL', sc, '1', key)!r}")
    s.close()
