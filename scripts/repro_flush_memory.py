#!/usr/bin/env python3
"""Repro: does FLUSHDB actually release memory after loading a large .kdb snapshot?

Starts a kividb server in a scratch dir with the snapshot as dump.kdb, then walks
through load -> FLUSHDB -> FLUSHALL -> MEMORY PURGE, reporting at each step both
what kividb claims (INFO memory) and what the OS says the process is holding.

Usage: repro_flush_memory.py --bin <kividb> --snapshot <file.kdb> [--port 6390]
"""
import argparse
import os
import shutil
import socket
import subprocess
import sys
import time


class Resp:
    def __init__(self, host, port, timeout=600):
        self.s = socket.create_connection((host, port), timeout=timeout)
        self.f = self.s.makefile("rb")

    def cmd(self, *args):
        out = b"*%d\r\n" % len(args)
        for a in args:
            b = a.encode() if isinstance(a, str) else a
            out += b"$%d\r\n%s\r\n" % (len(b), b)
        self.s.sendall(out)
        return self._read()

    def _read(self):
        line = self.f.readline()
        if not line:
            raise EOFError("connection closed")
        t, rest = line[:1], line[1:-2]
        if t == b"+":
            return rest.decode()
        if t == b"-":
            raise RuntimeError(rest.decode())
        if t == b":":
            return int(rest)
        if t == b"$":
            n = int(rest)
            if n == -1:
                return None
            data = self.f.read(n + 2)[:-2]
            return data.decode("utf-8", "replace")
        if t == b"*":
            return [self._read() for _ in range(int(rest))]
        raise RuntimeError(f"bad reply {line!r}")

    def close(self):
        try:
            self.s.close()
        except OSError:
            pass


def proc_rss(pid):
    """OS view of resident memory, independent of what the server reports."""
    if sys.platform.startswith("linux"):
        try:
            with open(f"/proc/{pid}/status") as fh:
                for line in fh:
                    if line.startswith("VmRSS:"):
                        return int(line.split()[1]) * 1024
        except OSError:
            pass
        return 0
    if sys.platform == "win32":
        out = subprocess.run(
            ["powershell", "-NoProfile", "-Command",
             f"(Get-Process -Id {pid}).WorkingSet64"],
            capture_output=True, text=True)
        try:
            return int(out.stdout.strip())
        except ValueError:
            return 0
    return 0


def mb(n):
    return f"{n / (1024 * 1024):8.1f} MB"


def info_memory(c):
    raw = c.cmd("INFO", "memory") or ""
    d = {}
    for line in raw.replace("\r", "").split("\n"):
        if ":" in line and not line.startswith("#"):
            k, v = line.split(":", 1)
            d[k] = v
    return d


def report(label, c, pid):
    d = info_memory(c)
    used = int(d.get("used_memory", 0))
    peak = int(d.get("used_memory_peak", 0))
    dbsize = c.cmd("DBSIZE")
    rss = proc_rss(pid)
    print(f"{label:<28} dbsize={dbsize:<10} used_memory={mb(used)}   "
          f"os_rss={mb(rss)}   peak={mb(peak)}")
    return used, rss


def start(binary, workdir, port, snapshot=None):
    if os.path.isdir(workdir):
        shutil.rmtree(workdir, ignore_errors=True)
    os.makedirs(workdir, exist_ok=True)
    if snapshot:
        shutil.copy(snapshot, os.path.join(workdir, "dump.kdb"))
    log = open(os.path.join(workdir, "server.log"), "wb")
    p = subprocess.Popen([binary, "--port", str(port)], cwd=workdir,
                         stdout=log, stderr=subprocess.STDOUT)
    return p


def wait_ready(port, proc, timeout=600):
    t0 = time.time()
    while time.time() - t0 < timeout:
        if proc.poll() is not None:
            raise RuntimeError(f"server exited with {proc.returncode}")
        try:
            c = Resp("127.0.0.1", port)
            if c.cmd("PING") == "PONG":
                return c, time.time() - t0
            c.close()
        except (OSError, EOFError, RuntimeError):
            pass
        time.sleep(0.5)
    raise TimeoutError("server never became ready")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin", required=True)
    ap.add_argument("--snapshot", required=True)
    ap.add_argument("--port", type=int, default=6390)
    ap.add_argument("--workdir", default=None)
    a = ap.parse_args()

    work = a.workdir or os.path.join(os.path.expanduser("~"), "kividb-repro-run")
    snap_size = os.path.getsize(a.snapshot)
    print(f"binary   : {a.bin}")
    print(f"snapshot : {a.snapshot} ({mb(snap_size)})")
    print(f"platform : {sys.platform}\n")

    # ── baseline: empty server, no snapshot ──────────────────────────────
    bp = start(a.bin, work + "-base", a.port + 1)
    bc, _ = wait_ready(a.port + 1, bp)
    _, base_rss = report("0. empty server (baseline)", bc, bp.pid)
    bc.close()
    bp.terminate()
    print()

    # ── loaded server ────────────────────────────────────────────────────
    p = start(a.bin, work, a.port, a.snapshot)
    c, load_secs = wait_ready(a.port, p)
    time.sleep(3)
    print(f"(snapshot loaded in {load_secs:.1f}s)")
    used_loaded, rss_loaded = report("1. after load", c, p.pid)

    c.cmd("FLUSHDB")
    time.sleep(3)
    used_flushdb, rss_flushdb = report("2. after FLUSHDB", c, p.pid)

    c.cmd("FLUSHALL")
    time.sleep(3)
    report("3. after FLUSHALL", c, p.pid)

    c.cmd("MEMORY", "PURGE")
    time.sleep(3)
    used_purge, rss_purge = report("4. after MEMORY PURGE", c, p.pid)

    print()
    print("--- accounting ---")
    print(f"{'empty server baseline':<44}{mb(base_rss)}")
    print(f"{'peak while loaded':<44}{mb(rss_loaded)}")
    print(f"{'FLUSHDB released (os_rss)':<44}{mb(rss_loaded - rss_flushdb)}")
    print(f"{'FLUSHDB released (used_memory reported)':<44}{mb(used_loaded - used_flushdb)}")
    print(f"{'MEMORY PURGE additionally released':<44}{mb(rss_flushdb - rss_purge)}")
    print(f"{'still held vs empty baseline':<44}{mb(rss_purge - base_rss)}")

    c.close()
    p.terminate()
    print("\n=== REPRO DONE ===")


if __name__ == "__main__":
    main()
