"""Servers a suite starts itself: several nodes, a cluster, and the Redis tools.

For configurations declared with Config(..., self_hosted=True): run-all.sh
starts no server and passes the engine's binary as ctx.bin.

    with Nodes(ctx) as ns:
        a = ns.start()                      # one more server like run-all.sh's
        nodes = ns.cluster(3)               # 3 masters sharing the slots evenly
        ...
    # every node is stopped on exit, on an exception and on SIGTERM (timeout)

    slot(b"key")                            # the key's hash slot (CRC16/XMODEM, hash tags)
    redis_tool("redis-benchmark")           # next to $REGRESS_REDIS_SERVER
    redis_cli_7()                           # a redis-cli 7.0.x (see its docstring)

Each node's pid is also written to $REGRESS_PIDDIR/<pid> (the engine binary's
path inside), which run-all.sh uses to kill what a SIGKILLed suite left behind.

Node ports are picked below the ephemeral range, and for cluster nodes the bus
port (port + 10000) too.
"""
import binascii
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time

from ports import ephemeral_range

_LIVE = []
_OWNER = os.getpid()  # forked helpers (probes, workers) must not stop the nodes


def _stop_all(*_):
    if os.getpid() != _OWNER:
        return
    for n in list(_LIVE):
        n.stop()


def _on_term(signum, frame):
    if os.getpid() != _OWNER:
        os._exit(128 + signum)
    _stop_all()
    sys.exit(128 + signum)


signal.signal(signal.SIGTERM, _on_term)


def _port_free(port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        if s.connect_ex(("127.0.0.1", port)) == 0:
            return False
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.bind(("127.0.0.1", port))
        except OSError:
            return False
    return True


def free_port(bus=False, tries=500):
    import random
    elo, ehi = ephemeral_range()
    rng = random.SystemRandom()
    lo, hi = (11000, 20000) if bus else (20000, 30000)
    for _ in range(tries):
        p = rng.randrange(lo, hi)
        ports = (p, p + 10000) if bus else (p,)
        if any(elo <= q <= ehi for q in ports):
            continue
        if all(_port_free(q) for q in ports):
            return p
    raise RuntimeError("no free port below the ephemeral range")


def slot(key):
    if isinstance(key, str):
        key = key.encode()
    s = key.find(b"{")
    if s >= 0:
        e = key.find(b"}", s + 1)
        if e > s + 1:
            key = key[s + 1:e]
    return binascii.crc_hqx(key, 0) & 16383


def redis_tool(name):
    srv = os.environ.get("REGRESS_REDIS_SERVER", "")
    for p in (os.path.join(os.path.dirname(srv), name) if srv else "", shutil.which(name) or ""):
        if p and os.access(p, os.X_OK):
            return p
    raise RuntimeError("%s not found next to $REGRESS_REDIS_SERVER or on PATH" % name)


def redis_benchmark_for(port, seconds, args):
    """Requests a second of redis-benchmark <args> run for a fixed time, not a
    fixed count (a count can take minutes on a build where the command waits):
    it is stopped with SIGINT after `seconds` and the last progress line's
    "overall" rate is taken. 0.0 if it printed none."""
    import re
    cmd = ["timeout", "-s", "INT", str(seconds), redis_tool("redis-benchmark"), "-p", str(port), "-q",
           "-n", "1000000000"] + list(args)
    out = subprocess.run(cmd, capture_output=True, text=True, timeout=seconds + 60).stdout
    m = re.findall(r"rps=[\d.]+ \(overall: ([\d.]+)\)", out.replace("\r", "\n"))
    return float(m[-1]) if m else 0.0


def _cli_version(p):
    try:
        out = subprocess.run([p, "--version"], capture_output=True, text=True, timeout=10).stdout
        return out.split()[1] if out.startswith("redis-cli") else ""
    except Exception:
        return ""


def redis_cli_7():
    """A redis-cli 7.0.x, whose `--cluster reshard` takes the GETKEYSINSLOT +
    MIGRATE path (redis-cli 8.x uses CLUSTER MIGRATION IMPORT against KiviDB).

    $REDIS_CLI_7, else /usr/bin/redis-cli if it is 7.0.x (Ubuntu 24.04's
    redis-tools is 7.0.15), else redis-cli built from Redis's 7.0.15 tag into
    $REGRESS_CACHE/redis-cli-7.0.15."""
    for p in (os.environ.get("REDIS_CLI_7", ""), "/usr/bin/redis-cli"):
        if p and os.access(p, os.X_OK) and _cli_version(p).startswith("7.0."):
            return p
    cache = os.environ.get("REGRESS_CACHE", os.path.expanduser("~/.cache/kividb-regress"))
    d = os.path.join(cache, "redis-cli-7.0.15")
    p = os.path.join(d, "src", "redis-cli")
    if os.access(p, os.X_OK) and _cli_version(p).startswith("7.0."):
        return p
    print("[nodes] building redis-cli 7.0.15 from its tag", flush=True)
    shutil.rmtree(d, ignore_errors=True)
    subprocess.run(["git", "clone", "-q", "--depth", "1", "--branch", "7.0.15",
                    "https://github.com/redis/redis.git", d], check=True)
    subprocess.run(["make", "-C", d, "-j%d" % (os.cpu_count() or 2), "BUILD_TLS=no", "redis-cli"],
                   check=True, stdout=subprocess.DEVNULL)
    return p


class Node:
    def __init__(self, kind, bin, io="auto", threads=0, cluster=False, args=(), env=None):
        self.kind, self.bin, self.cluster = kind, bin, cluster
        self.port = free_port(bus=cluster)
        self.dir = tempfile.mkdtemp(prefix="kividb-regress-node.")
        self.log_path = os.path.join(self.dir, "server.log")
        if kind == "redis":
            cmd = [bin, "--port", str(self.port), "--bind", "127.0.0.1", "--save", "", "--appendonly", "no",
                   "--enable-debug-command", "yes", "--dir", self.dir]
            if cluster:
                cmd += ["--cluster-enabled", "yes", "--cluster-config-file", "nodes.conf",
                        "--cluster-node-timeout", "5000"]
        else:
            cmd = [bin, "--port", str(self.port), "--bind", "127.0.0.1", "--protected-mode", "no",
                   "--dir", self.dir]
            if io != "auto":
                cmd += ["--io-model", io]
            if threads:
                cmd += ["--threads", str(threads)]
            if cluster:
                cmd += ["--cluster-enabled", "yes", "--cluster-config-file", "nodes.conf",
                        "--cluster-node-timeout", "5000"]
        cmd += list(args)
        e = dict(os.environ, KIVIDB_NO_UPDATE_CHECK="1")
        e.update(env or {})
        self.log = open(self.log_path, "wb")
        self.proc = None
        self._pidfile = None
        try:
            self.proc = subprocess.Popen(cmd, cwd=self.dir, stdin=subprocess.DEVNULL, stdout=self.log,
                                         stderr=subprocess.STDOUT, env=e)
        except BaseException:
            self.log.close()
            shutil.rmtree(self.dir, ignore_errors=True)
            raise
        self.pid = self.proc.pid
        piddir = os.environ.get("REGRESS_PIDDIR")
        if piddir:
            try:
                self._pidfile = os.path.join(piddir, str(self.pid))
                with open(self._pidfile, "w") as f:
                    f.write(bin)
            except OSError:
                self._pidfile = None
        _LIVE.append(self)
        deadline = time.time() + 60
        while time.time() < deadline:
            if self.proc.poll() is not None:
                break
            if self._ping():
                return
            time.sleep(0.1)
        tail = self.log_tail(2000)
        self.cleanup()  # stops it, closes the log, removes the directory
        raise RuntimeError("%s on port %d did not answer PING: %s" % (kind, self.port, tail))

    def _ping(self):
        try:
            with socket.create_connection(("127.0.0.1", self.port), timeout=2) as s:
                s.sendall(b"PING\r\n")
                return s.recv(16).startswith(b"+PONG")
        except OSError:
            return False

    def client(self, **kw):
        import redis
        kw.setdefault("socket_timeout", 600)
        return redis.Redis(host="127.0.0.1", port=self.port, **kw)

    def log_tail(self, n=65536):
        try:
            with open(self.log_path, "rb") as f:
                f.seek(0, 2)
                f.seek(max(0, f.tell() - n))
                return f.read().decode("utf-8", "replace")
        except OSError:
            return ""

    def stop(self):
        if self.proc.poll() is None:
            try:
                with socket.create_connection(("127.0.0.1", self.port), timeout=2) as s:
                    s.sendall(b"SHUTDOWN NOSAVE\r\n")
                    s.settimeout(2)
                    try:
                        s.recv(64)
                    except OSError:
                        pass
            except OSError:
                pass
            try:
                self.proc.wait(10)
            except subprocess.TimeoutExpired:
                self.proc.terminate()
                try:
                    self.proc.wait(5)
                except subprocess.TimeoutExpired:
                    self.proc.kill()
                    self.proc.wait()
        if self in _LIVE:
            _LIVE.remove(self)
        if self._pidfile:
            try:
                os.unlink(self._pidfile)
            except OSError:
                pass
            self._pidfile = None
        try:
            self.log.close()
        except Exception:
            pass

    def cleanup(self):
        self.stop()
        shutil.rmtree(self.dir, ignore_errors=True)


class Nodes:
    """The nodes of one engine in one configuration; all stopped on exit, and
    their log tails kept in ctx.raw["node_logs"]."""

    def __init__(self, ctx):
        self.ctx, self.nodes = ctx, []

    def start(self, cluster=False, args=(), io=None, threads=None):
        cfg = self.ctx.config
        n = Node(self.ctx.kind, self.ctx.bin, io or cfg.io, cfg.threads if threads is None else threads,
                 cluster=cluster, args=args)
        self.nodes.append(n)
        return n

    def cluster(self, masters, args=(), timeout=60):
        nodes = [self.start(cluster=True, args=args) for _ in range(masters)]
        per = 16384 // masters
        for i, n in enumerate(nodes):
            lo = i * per
            hi = 16383 if i == masters - 1 else (i + 1) * per - 1
            n.client().execute_command("CLUSTER", "ADDSLOTSRANGE", lo, hi)
            n.slots = (lo, hi)
        for n in nodes[1:]:
            nodes[0].client().execute_command("CLUSTER", "MEET", "127.0.0.1", n.port)
        deadline = time.time() + timeout
        while time.time() < deadline:
            ok = True
            for n in nodes:
                i = n.client().execute_command("CLUSTER", "INFO")
                i = i.decode() if isinstance(i, bytes) else i
                d = dict(l.split(":", 1) for l in i.split("\r\n") if ":" in l)
                if d.get("cluster_state") != "ok" or d.get("cluster_known_nodes") != str(masters):
                    ok = False
                    break
            if ok:
                return nodes
            time.sleep(0.2)
        raise RuntimeError("cluster of %d did not converge in %d s" % (masters, timeout))

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        logs = {}
        for i, n in enumerate(self.nodes):
            n.stop()
            logs["node%d-%d" % (i, n.port)] = n.log_tail(16384)
            n.cleanup()
        self.ctx.raw["node_logs"] = logs
        return False
