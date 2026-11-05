#!/usr/bin/env python3
"""Probe: does a MIGRATE stopped at the migrate-before-delete failpoint stop
the source node from answering anyone else?

Two standalone nodes (--io-model tokio --threads 2, as tests/cluster.rs runs
them). For each trial: SET a key on the source, send MIGRATE to the
destination from a fresh connection -- either at once after connecting (as
the test does) or after waiting `delay` seconds first, so that the server's
connection task has already found nothing to read and is parked on the I/O
driver -- wait for the failpoint file, then PING the source from another
fresh connection with a 3 s timeout, then remove the failpoint file.

usage: probe.py <kividb binary> <trials> <delay seconds> [extra args...]
"""
import os, socket, subprocess, sys, tempfile, time

binary, trials, delay = sys.argv[1], int(sys.argv[2]), float(sys.argv[3])
extra = sys.argv[4:]

def enc(*args):
    out = b"*%d\r\n" % len(args)
    for a in args:
        a = a if isinstance(a, bytes) else str(a).encode()
        out += b"$%d\r\n%s\r\n" % (len(a), a)
    return out

def start(port, env=None):
    d = tempfile.mkdtemp(prefix="probe")
    e = dict(os.environ)
    e.update(env or {})
    p = subprocess.Popen([binary, "--port", str(port), "--bind", "127.0.0.1",
                          "--io-model", "tokio", "--threads", "2", *extra],
                         cwd=d, env=e, stdout=subprocess.DEVNULL,
                         stderr=open(os.path.join(d, "node.log"), "w"))
    for _ in range(200):
        try:
            socket.create_connection(("127.0.0.1", port), 0.2).close()
            return p, d
        except OSError:
            time.sleep(0.05)
    raise SystemExit("node did not start")

def call(port, *args, timeout=10.0):
    s = socket.create_connection(("127.0.0.1", port), 2)
    s.settimeout(timeout)
    s.sendall(enc(*args))
    data = s.recv(4096)
    s.close()
    return data

src_port, dst_port = 17311, 17312
src, src_dir = start(src_port, {"KIVIDB_TEST_FAILPOINTS": "migrate-before-delete"})
dst, _ = start(dst_port)
stalls = 0
try:
    for hit in range(1, trials + 1):
        key = "k%d" % hit
        assert call(src_port, "SET", key, "v").startswith(b"+OK")
        m = socket.create_connection(("127.0.0.1", src_port), 2)
        m.settimeout(30)
        if delay:
            time.sleep(delay)
        m.sendall(enc("MIGRATE", "127.0.0.1", dst_port, key, 0, 5000))
        fp = os.path.join(src_dir, "failpoint.migrate-before-delete.%d" % hit)
        t0 = time.time()
        while not os.path.exists(fp):
            if time.time() - t0 > 10:
                raise SystemExit("failpoint %d never reached" % hit)
            time.sleep(0.005)
        t1 = time.time()
        try:
            reply = call(src_port, "PING", timeout=3)
            ok = reply.startswith(b"+PONG")
        except OSError:
            ok = False
        waited = time.time() - t1
        if not ok and os.environ.get("DUMP") and not os.path.exists(os.environ["DUMP"]):
            # A PING is waiting on a fresh connection now, as well.
            pend = socket.create_connection(("127.0.0.1", src_port), 2)
            pend.sendall(enc("PING"))
            time.sleep(0.2)
            with open(os.environ["DUMP"], "w") as f:
                f.write("probe stall, trial %d, src pid %d\n\n" % (hit, src.pid))
                for t in sorted(os.listdir("/proc/%d/task" % src.pid), key=int):
                    base = "/proc/%d/task/%s/" % (src.pid, t)
                    comm = open(base + "comm").read().strip()
                    state = open(base + "stat").read().split(") ")[1].split()[0]
                    wchan = open(base + "wchan").read().strip()
                    f.write("%s %s %s %s\n" % (t, comm, state, wchan))
                f.write("\n== ss (Recv-Q > 0: bytes nobody has read)\n")
                f.flush()
                subprocess.run("ss -tan '( sport = :%d )'" % src_port, shell=True, stdout=f, stderr=f)
                f.write("\n== gdb thread apply all bt\n")
                f.flush()
                subprocess.run(["sudo", "timeout", "60", "gdb", "-p", str(src.pid), "-batch",
                                "-ex", "set pagination off", "-ex", "thread apply all bt"],
                               stdout=f, stderr=subprocess.STDOUT)
            pend.close()
        os.remove(fp)
        mreply = m.recv(4096)
        m.close()
        if not ok:
            stalls += 1
        print("trial %d: PING %s after %.3fs; MIGRATE replied %r"
              % (hit, "answered" if ok else "NOT answered", waited, mreply[:40]),
              flush=True)
finally:
    src.kill(); dst.kill()
print("delay=%s stalls=%d/%d" % (delay, stalls, trials))
