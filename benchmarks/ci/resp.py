"""A small RESP client for the harnesses that drive the server directly (no packages to install).

    c = Conn(port)                  # RESP2; Conn(port, resp3=True) sends HELLO 3
    c.cmd("SET", "k", "v")          # -> "OK" (simple strings and bulk strings come back as str,
                                    #    integers as int, nil as None, arrays as lists)
    c.cmd_raw(...)                  # the same with bulk strings as bytes
    c.read(timeout=1.0)             # the next reply or push; raises Timeout when none comes
    c.send(...)                     # a command without reading its reply

An error reply comes back as an `Err` (a str subclass) instead of being raised, so a check
can compare it. Checks are rows for compat_junit.write_junit: `Checks().run(name, fn)`.
"""
import socket
import time
import traceback


class Timeout(Exception):
    pass


class Err(str):
    """An error reply."""


class Conn:
    def __init__(self, port, host="127.0.0.1", resp3=False, timeout=10.0):
        self.sock = socket.create_connection((host, port), timeout=timeout)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.buf = b""
        self.raw = False
        if resp3:
            self.cmd("HELLO", "3")

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass

    @staticmethod
    def encode(args):
        out = [b"*%d\r\n" % len(args)]
        for a in args:
            if isinstance(a, str):
                a = a.encode()
            elif isinstance(a, (int, float)):
                a = str(a).encode()
            out.append(b"$%d\r\n%s\r\n" % (len(a), a))
        return b"".join(out)

    def send(self, *args):
        self.sock.sendall(self.encode(args))

    def cmd(self, *args, timeout=10.0):
        self.raw = False
        self.send(*args)
        return self.read(timeout)

    def cmd_raw(self, *args, timeout=10.0):
        self.raw = True
        try:
            self.send(*args)
            return self.read(timeout)
        finally:
            self.raw = False

    def _fill(self, deadline):
        left = deadline - time.monotonic()
        if left <= 0:
            raise Timeout()
        self.sock.settimeout(left)
        try:
            data = self.sock.recv(1 << 16)
        except socket.timeout:
            raise Timeout()
        if not data:
            raise ConnectionError("connection closed")
        self.buf += data

    def _line(self, deadline):
        while b"\r\n" not in self.buf:
            self._fill(deadline)
        line, self.buf = self.buf.split(b"\r\n", 1)
        return line

    def _take(self, n, deadline):
        while len(self.buf) < n + 2:
            self._fill(deadline)
        data, self.buf = self.buf[:n], self.buf[n + 2:]
        return data

    def read(self, timeout=10.0):
        return self._reply(time.monotonic() + timeout)

    def _str(self, data):
        return data if self.raw else data.decode("utf-8", "replace")

    def _reply(self, deadline):
        line = self._line(deadline)
        t, rest = line[:1], line[1:]
        if t == b"+":
            return self._str(rest)
        if t == b"-":
            return Err(rest.decode("utf-8", "replace"))
        if t == b":":
            return int(rest)
        if t == b"$":
            n = int(rest)
            return None if n < 0 else self._str(self._take(n, deadline))
        if t in (b"*", b"~", b">"):
            n = int(rest)
            if n < 0:
                return None
            return [self._reply(deadline) for _ in range(n)]
        if t == b"%":
            n = int(rest)
            out = {}
            for _ in range(n):
                k = self._reply(deadline)
                out[k] = self._reply(deadline)
            return out
        if t == b"_":
            return None
        if t == b"#":
            return rest == b"t"
        if t == b",":
            return float(rest)
        if t == b"=":
            n = int(rest)
            return self._str(self._take(n, deadline))[4:]
        raise ValueError("unexpected reply %r" % line)

    def quiet_for(self, seconds):
        """True if nothing arrives in `seconds`."""
        try:
            self.read(seconds)
        except Timeout:
            return True
        return False


class Checks:
    """The rows of a harness: each check is a function that returns None (pass) or raises
    AssertionError(detail) (fail); any other exception is a fail with its traceback."""

    def __init__(self):
        self.rows = []

    def run(self, name, fn, *args):
        t = time.monotonic()
        status, detail = "PASS", ""
        try:
            fn(*args)
        except AssertionError as e:
            status, detail = "FAIL", str(e) or "assertion failed"
        except Exception:
            status, detail = "FAIL", traceback.format_exc(limit=3)
        self.rows.append({"check": name, "status": status, "detail": detail,
                          "ms": (time.monotonic() - t) * 1000})
        print("%s  %s%s" % (status, name, "" if status == "PASS" else "  <- " + detail.splitlines()[-1][:300]), flush=True)

    def failed(self):
        return sum(1 for r in self.rows if r["status"] != "PASS")


def eq(got, want, what=""):
    assert got == want, "%s: got %r, want %r" % (what or "value", got, want)
