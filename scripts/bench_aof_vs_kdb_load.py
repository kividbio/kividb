#!/usr/bin/env python3
"""Compare startup cost for the same dataset loaded from a .kdb snapshot versus
from an append-only file.

Both load paths run before the listener is bound, so time-to-PONG is the load
time. Peak RSS comes from VmHWM, which also captures the transient spike the
replay allocates on top of the dataset itself.

Usage: bench_aof_vs_kdb_load.py [binary] [snapshot]
"""

import os
import shutil
import socket
import subprocess
import sys
import time

WIN = sys.platform == "win32"
REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

BIN = sys.argv[1] if len(sys.argv) > 1 else (
    os.path.join(REPO, "target", "release", "kividb.exe") if WIN
    else os.path.expanduser("~/kividb-repro/target/release/kividb"))
SNAP = sys.argv[2] if len(sys.argv) > 2 else os.path.join(
    REPO, "kdb-export-mt082qty.kdb")
WORK = os.path.join(os.path.expanduser("~"), "kividb-aof-bench")
PORT = int(os.environ.get("PORT", "6440"))


def mb(n):
    return f"{n / (1024 * 1024):.1f} MB"


class Client:
    def __init__(self, port):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=600)
        self.buf = b""

    def cmd(self, *args):
        out = [f"*{len(args)}\r\n".encode()]
        for a in args:
            a = a.encode() if isinstance(a, str) else a
            out.append(b"$%d\r\n%s\r\n" % (len(a), a))
        self.sock.sendall(b"".join(out))
        return self._read()

    def _line(self):
        while b"\r\n" not in self.buf:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise EOFError
            self.buf += chunk
        line, self.buf = self.buf.split(b"\r\n", 1)
        return line

    def _read(self):
        line = self._line()
        tag, rest = line[:1], line[1:]
        if tag in b"+-:":
            return rest.decode(errors="replace")
        if tag == b"$":
            n = int(rest)
            if n < 0:
                return None
            while len(self.buf) < n + 2:
                self.buf += self.sock.recv(65536)
            val, self.buf = self.buf[:n], self.buf[n + 2:]
            return val.decode(errors="replace")
        if tag == b"*":
            return [self._read() for _ in range(int(rest))]
        return line.decode(errors="replace")


def vm(pid, field):
    """Current ("VmRSS") or peak ("VmHWM") resident memory of `pid`, in bytes."""
    if WIN:
        import ctypes
        from ctypes import wintypes

        class Counters(ctypes.Structure):
            _fields_ = [("cb", wintypes.DWORD), ("PageFaultCount", wintypes.DWORD)] + [
                (n, ctypes.c_size_t) for n in (
                    "PeakWorkingSetSize", "WorkingSetSize",
                    "QuotaPeakPagedPoolUsage", "QuotaPagedPoolUsage",
                    "QuotaPeakNonPagedPoolUsage", "QuotaNonPagedPoolUsage",
                    "PagefileUsage", "PeakPagefileUsage")
            ]

        handle = ctypes.windll.kernel32.OpenProcess(0x1000, False, pid)
        if not handle:
            return 0
        try:
            c = Counters()
            c.cb = ctypes.sizeof(c)
            if not ctypes.windll.psapi.GetProcessMemoryInfo(
                    handle, ctypes.byref(c), c.cb):
                return 0
            return c.WorkingSetSize if field == "VmRSS" else c.PeakWorkingSetSize
        finally:
            ctypes.windll.kernel32.CloseHandle(handle)

    try:
        with open(f"/proc/{pid}/status") as f:
            for line in f:
                if line.startswith(field):
                    return int(line.split()[1]) * 1024
    except OSError:
        pass
    return 0


def start(*extra):
    """Launch the server; return (proc, seconds_until_serving)."""
    log = open(os.path.join(WORK, "server.log"), "ab")
    t0 = time.monotonic()
    proc = subprocess.Popen([BIN, "--port", str(PORT), *extra],
                            cwd=WORK, stdout=log, stderr=log)
    deadline = t0 + 900
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise SystemExit(f"server exited early ({proc.returncode}); see {WORK}/server.log")
        try:
            c = Client(PORT)
            if c.cmd("PING") == "PONG":
                elapsed = time.monotonic() - t0
                c.sock.close()
                return proc, elapsed
            c.sock.close()
        except OSError:
            time.sleep(0.05)
    raise SystemExit("server never became reachable")


def stop(proc):
    try:
        c = Client(PORT)
        c.cmd("SHUTDOWN", "NOSAVE")
    except Exception:
        pass
    try:
        proc.wait(timeout=60)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
    time.sleep(1)


def main():
    shutil.rmtree(WORK, ignore_errors=True)
    os.makedirs(WORK)
    shutil.copy(SNAP, os.path.join(WORK, "dump.kdb"))
    snap_size = os.path.getsize(os.path.join(WORK, "dump.kdb"))

    print(f"binary  : {BIN}")
    print(f"snapshot: {mb(snap_size)}\n")

    # 1. snapshot load
    proc, secs = start()
    c = Client(PORT)
    keys = c.cmd("DBSIZE")
    rss, peak = vm(proc.pid, "VmRSS"), vm(proc.pid, "VmHWM")
    c.sock.close()
    print(f"{'load from dump.kdb':<26} {secs:7.2f}s  keys={keys:<10} "
          f"rss={mb(rss):<10} peak={mb(peak)}")
    kdb_secs = secs
    stop(proc)

    # 2. build an AOF holding the same dataset
    print("\nbuilding appendonly.aof for the same dataset...")
    proc, _ = start("--aof")
    c = Client(PORT)
    aof_path = os.path.join(WORK, "appendonly.aof")
    c.cmd("BGREWRITEAOF")
    # The rewrite writes a temp file and hands it to the writer task to swap in,
    # so "in_progress: 0" can precede the swap. Wait for the swap itself.
    stable, last = 0, -1
    for _ in range(1800):
        time.sleep(0.5)
        size = os.path.getsize(aof_path) if os.path.exists(aof_path) else 0
        stable = stable + 1 if size == last and size > 1000 else 0
        last = size
        if stable >= 4:
            break
    c.sock.close()
    stop(proc)

    aof_size = os.path.getsize(aof_path) if os.path.exists(aof_path) else 0
    print(f"appendonly.aof: {mb(aof_size)}")
    if aof_size < 1000:
        raise SystemExit("AOF was not produced")

    # 3. AOF recovery, with the snapshot moved aside so it cannot win
    os.rename(os.path.join(WORK, "dump.kdb"), os.path.join(WORK, "dump.kdb.bak"))
    proc, secs = start("--aof")
    c = Client(PORT)
    keys = c.cmd("DBSIZE")
    rss, peak = vm(proc.pid, "VmRSS"), vm(proc.pid, "VmHWM")
    c.sock.close()
    print(f"\n{'load from appendonly.aof':<26} {secs:7.2f}s  keys={keys:<10} "
          f"rss={mb(rss):<10} peak={mb(peak)}")
    stop(proc)

    with open(os.path.join(WORK, "server.log"), errors="replace") as f:
        for line in f:
            if "AOF replay" in line:
                print(line.rstrip())

    print(f"\nAOF is {secs / kdb_secs:.1f}x slower to load than the snapshot")
    print("=== AOF LOAD BENCH DONE ===")


if __name__ == "__main__":
    main()
