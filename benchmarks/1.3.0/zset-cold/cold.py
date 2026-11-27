#!/usr/bin/env python3
"""Cold-cache sorted-set micro-benchmark: server CPU (utime+stime from /proc)
per command, random keys, working set many times L2+L3.

usage: cold.py <label> <bin> [reps] [sizes...]
Writes JSON lines to cold-<label>.jsonl.
"""
import json, os, socket, subprocess, sys, time

ZLOAD = os.path.expanduser("~/zload")
TOTAL = int(os.environ.get("TOTAL", 8_000_000))
COUNT = int(os.environ.get("COUNT", 1_000_000))
CASES = ["zscore_hit", "zscore_miss", "zrank", "zrange", "zrevrange", "zincrby", "zadd_zrem", "feed"]
TICK = os.sysconf("SC_CLK_TCK")


def cmd(port, *a):
    s = socket.create_connection(("127.0.0.1", port))
    b = b"*%d\r\n" % len(a) + b"".join(b"$%d\r\n%s\r\n" % (len(x), x) for x in (str(y).encode() for y in a))
    s.sendall(b)
    time.sleep(0.05)
    out = s.recv(1 << 20)
    s.close()
    return out


def used_memory(port):
    txt = cmd(port, "INFO", "memory").decode()
    for line in txt.split("\r\n"):
        if line.startswith("used_memory:"):
            return int(line.split(":")[1])


def settled(port):
    low = used_memory(port)
    since = time.time()
    end = time.time() + 60
    while time.time() < end and time.time() - since < 2:
        time.sleep(0.1)
        now = used_memory(port)
        if now < low - 4096:
            since = time.time()
        low = min(low, now)
    return low


def cpu(pid):
    f = open("/proc/%d/stat" % pid).read().rsplit(")", 1)[1].split()
    return (int(f[11]) + int(f[12])) / TICK


def run(label, binary, reps, sizes):
    out = open("cold-%s.jsonl" % label, "a")
    port = 6390
    for n in sizes:
        keys = max(1, TOTAL // n)
        for rep in range(reps):
            d = "/tmp/cold-%s-%d-%d-%d" % (label, n, rep, time.time_ns())
            os.makedirs(d, exist_ok=True)
            open(d + "/k.conf", "w").write("enable-debug-command yes\n")
            srv = subprocess.Popen(["taskset", "-c", "0,1", binary, "--port", str(port), "--bind", "127.0.0.1",
                                    "--protected-mode", "no", "--dir", d, "--configfile", d + "/k.conf",
                                    "--threads", "2", "--save", "", "--appendonly", "no"],
                                   stdout=open(d + "/log", "w"), stderr=subprocess.STDOUT,
                                   env=dict(os.environ, KIVIDB_NO_UPDATE_CHECK="1"), cwd=d)
            for _ in range(100):
                try:
                    if b"PONG" in cmd(port, "PING"):
                        break
                except OSError:
                    time.sleep(0.1)
            info = cmd(port, 'INFO', 'server').decode()
            assert ('process_id:%d' % srv.pid) in info, 'another server on the port'
            u0 = settled(port)
            t = time.time()
            subprocess.run(["taskset", "-c", "2-7", ZLOAD, str(port), "load", str(keys), str(n), "0", "1", "64", "1"], check=True)
            load_s = time.time() - t
            u1 = settled(port)
            row = {"u0": u0, "u1": u1, "label": label, "n": n, "rep": rep, "keys": keys, "bytes_per_entry": (u1 - u0) / (keys * n), "load_s": load_s}
            for case in CASES:
                c0 = cpu(srv.pid)
                t = time.time()
                subprocess.run(["taskset", "-c", "2-7", ZLOAD, str(port), case, str(keys), str(n), str(COUNT), "4", "32", str(rep + 11)], check=True)
                wall = time.time() - t
                c1 = cpu(srv.pid)
                ncmd = COUNT * (2 if case in ("zadd_zrem", "feed") else 1)
                row[case + "_ns"] = (c1 - c0) * 1e9 / (COUNT // 4 * 4)  # per call (a pair for the two-command cases)
                row[case + "_wall"] = wall
            row["u_after"] = settled(port)
            # A save of the loaded shape, timed.
            t = time.time()
            cmd(port, "BGSAVE")
            peak = 0
            while True:
                txt = cmd(port, "INFO", "persistence").decode()
                if "rdb_bgsave_in_progress:0" in txt and time.time() - t > 0.2:
                    break
                time.sleep(0.05)
            row["bgsave_s"] = time.time() - t
            out.write(json.dumps(row) + "\n")
            out.flush()
            print(json.dumps(row), flush=True)
            try:
                cmd(port, "SHUTDOWN", "NOSAVE")
            except OSError:
                pass
            try:
                srv.wait(20)
            except subprocess.TimeoutExpired:
                srv.kill()
                srv.wait()


if __name__ == "__main__":
    label, binary = sys.argv[1], sys.argv[2]
    reps = int(sys.argv[3]) if len(sys.argv) > 3 else 5
    sizes = [int(x) for x in sys.argv[4:]] or [800, 128, 256, 2000, 50000]
    run(label, binary, reps, sizes)
