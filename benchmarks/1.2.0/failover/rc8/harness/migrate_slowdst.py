#!/usr/bin/env python3
"""MIGRATE to a destination that is slow to answer, with --io-model tokio.

A fake destination accepts MIGRATE's connection, reads what it is sent and answers nothing for `hold`
seconds, then answers +OK to every command. The source is a real KiviDB node (--io-model tokio
--threads 2). Each trial: SET a key on the source, open a fresh connection, wait `delay` s (so the
server's connection task is parked on the I/O driver, the case that stalled), send MIGRATE with a
`timeout_ms` timeout. While MIGRATE waits, every 0.25 s a PING goes out on a fresh connection (accept
+ read + reply) and on a connection opened before the MIGRATE, each with a 3 s timeout. A PING that
takes more than 1 s or is not answered counts as a stall.

usage: migrate_slowdst.py <kividb binary> <trials> <delay s> <hold s> <timeout ms>
"""
import os, socket, subprocess, sys, tempfile, threading, time

binary, trials, delay, hold, tmo = sys.argv[1], int(sys.argv[2]), float(sys.argv[3]), float(sys.argv[4]), int(sys.argv[5])
SRC, DST = 17411, 17412


def enc(*args):
    out = b"*%d\r\n" % len(args)
    for a in args:
        a = a if isinstance(a, bytes) else str(a).encode()
        out += b"$%d\r\n%s\r\n" % (len(a), a)
    return out


def fake_dst():
    ls = socket.socket(); ls.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    ls.bind(("127.0.0.1", DST)); ls.listen(16)

    def serve(c):
        t_end = time.time() + hold
        pending = 0
        try:
            while True:
                c.settimeout(0.05)
                try:
                    d = c.recv(65536)
                    if not d:
                        return
                    # count the commands that arrived (RESTORE / SELECT / AUTH ...); each gets +OK
                    pending += max(1, d.count(b"\r\n*") + (1 if d.startswith(b"*") else 0))
                except socket.timeout:
                    pass
                if pending and time.time() >= t_end:
                    c.sendall(b"+OK\r\n" * pending)
                    pending = 0
        except OSError:
            pass
    while True:
        c, _ = ls.accept()
        threading.Thread(target=serve, args=(c,), daemon=True).start()


threading.Thread(target=fake_dst, daemon=True).start()
d = tempfile.mkdtemp(prefix="slowdst")
p = subprocess.Popen([binary, "--port", str(SRC), "--bind", "127.0.0.1", "--io-model", "tokio", "--threads", "2"],
                     cwd=d, stdout=subprocess.DEVNULL, stderr=open(os.path.join(d, "node.log"), "w"))
for _ in range(200):
    try:
        socket.create_connection(("127.0.0.1", SRC), 0.2).close(); break
    except OSError:
        time.sleep(0.05)


def ping(sock=None):
    t = time.time()
    try:
        s = sock or socket.create_connection(("127.0.0.1", SRC), 3)
        s.settimeout(3)
        s.sendall(enc("PING"))
        ok = s.recv(64).startswith(b"+PONG")
        if sock is None:
            s.close()
    except OSError:
        ok = False
    return ok, time.time() - t


stalled_trials = 0
try:
    for hit in range(1, trials + 1):
        key = "k%d" % hit
        s = socket.create_connection(("127.0.0.1", SRC), 2); s.sendall(enc("SET", key, "v")); s.recv(64); s.close()
        old = socket.create_connection(("127.0.0.1", SRC), 2)
        m = socket.create_connection(("127.0.0.1", SRC), 2); m.settimeout(tmo / 1000 + 10)
        time.sleep(delay)
        t0 = time.time()
        m.sendall(enc("MIGRATE", "127.0.0.1", DST, key, 0, tmo))
        res = {}

        def wait_m():
            try:
                res["r"] = m.recv(4096)
            except OSError as e:
                res["r"] = repr(e).encode()
            res["t"] = time.time() - t0
        th = threading.Thread(target=wait_m); th.start()
        pings = []
        time.sleep(0.1)
        while th.is_alive() and time.time() - t0 < min(hold, tmo / 1000) - 0.2:
            pings.append(("fresh",) + ping())
            pings.append(("open",) + ping(old))
            time.sleep(0.25)
        th.join()
        bad = [x for x in pings if not x[1] or x[2] > 1.0]
        worst = max((x[2] for x in pings), default=0)
        if bad:
            stalled_trials += 1
        print("trial %d: MIGRATE replied %r after %.2fs; %d PINGs while it waited, %d stalled, worst %.3fs"
              % (hit, res.get("r", b"")[:40], res.get("t", -1), len(pings), len(bad), worst), flush=True)
        old.close(); m.close()
finally:
    p.kill()
print("delay=%s hold=%s timeout_ms=%d stalled_trials=%d/%d" % (delay, hold, tmo, stalled_trials, trials))
