#!/usr/bin/env python3
"""Once a second, record the engine's memory as the kernel and the engine see it.

    sudo python3 sampler.py <out.csv> <cgroup-dir> <main-pid> [interval-s]

Columns:
  t               unix time, seconds
  anon_mb         cgroup memory.stat "anon": private anonymous memory of every process in the
                  container. With a forked child (Redis/Valkey BGSAVE, AOF rewrite) the pages it shares
                  copy-on-write with the parent are counted once, and every page copied after the
                  fork is counted again: this is the physical memory the snapshot really costs.
  file_mb         cgroup page cache (the dump being written); not part of the engine's memory
  rss_main_mb     VmRSS of the main server process
  rss_all_mb      sum of VmRSS of every process in the container (double-counts shared COW pages)
  nproc           processes in the container (2 while Redis/Valkey have a child)
  used_mb         INFO used_memory (what the engine says it allocated)
  keys            sum of keys over all databases (INFO keyspace)
  bgsave, aofrw   rdb_bgsave_in_progress, aof_rewrite_in_progress (Dragonfly: saving)
  evicted, expired  INFO stats evicted_keys, expired_keys
  not_counted_mb  mem_not_counted_for_evict
  info_ms         how long INFO took to answer (a stalled server shows here); -1 = no answer within 2 s
Stops when the file /var/tmp/mem/sampler.stop appears or the main PID exits.
"""
import os, socket, sys, time

out, cg, pid = sys.argv[1], sys.argv[2], int(sys.argv[3])
interval = float(sys.argv[4]) if len(sys.argv) > 4 else 1.0
STOP = "/var/tmp/mem/sampler.stop"


def info():
    t0 = time.time()
    try:
        s = socket.create_connection(("127.0.0.1", 6379), timeout=2)
        s.sendall(b"*1\r\n$4\r\nINFO\r\n")
        buf = b""
        while True:
            chunk = s.recv(65536)
            if not chunk:
                break
            buf += chunk
            if buf.startswith(b"$"):
                hdr, _, rest = buf.partition(b"\r\n")
                if len(rest) >= int(hdr[1:]) + 2:
                    break
        s.close()
    except OSError:
        return {}, -1.0
    ms = (time.time() - t0) * 1000
    d = {}
    for line in buf.decode("latin-1").split("\r\n"):
        if ":" in line and not line.startswith("#"):
            k, _, v = line.partition(":")
            d[k] = v
    return d, ms


def kb(path, field):
    try:
        with open(path) as f:
            for line in f:
                if line.startswith(field):
                    return int(line.split()[1])
    except OSError:
        pass
    return 0


def memstat():
    d = {}
    try:
        with open(os.path.join(cg, "memory.stat")) as f:
            for line in f:
                k, v = line.split()
                d[k] = int(v)
    except OSError:
        pass
    return d


def procs():
    try:
        with open(os.path.join(cg, "cgroup.procs")) as f:
            return [int(x) for x in f.read().split()]
    except OSError:
        return []


MB = 1024 * 1024
cols = "t,anon_mb,file_mb,rss_main_mb,rss_all_mb,nproc,used_mb,keys,bgsave,aofrw,evicted,expired,not_counted_mb,info_ms"
new = not os.path.exists(out)
with open(out, "a", buffering=1) as f:
    if new:
        f.write(cols + "\n")
    nxt = time.time()
    while not os.path.exists(STOP) and os.path.exists(f"/proc/{pid}"):
        t = time.time()
        ms = memstat()
        ps = procs()
        rss_main = kb(f"/proc/{pid}/status", "VmRSS:") / 1024
        rss_all = sum(kb(f"/proc/{p}/status", "VmRSS:") for p in ps) / 1024
        d, ims = info()
        keys = 0
        for k, v in d.items():
            if k.startswith("db") and "keys=" in v:
                keys += int(v.split("keys=")[1].split(",")[0])
        bgsave = d.get("rdb_bgsave_in_progress", d.get("saving", ""))
        aofrw = d.get("aof_rewrite_in_progress", "")
        used = int(d["used_memory"]) / MB if "used_memory" in d else ""
        nc = int(d["mem_not_counted_for_evict"]) / MB if "mem_not_counted_for_evict" in d else ""
        row = [f"{t:.3f}", f"{ms.get('anon', 0) / MB:.1f}", f"{ms.get('file', 0) / MB:.1f}",
               f"{rss_main:.1f}", f"{rss_all:.1f}", str(len(ps)),
               f"{used:.1f}" if used != "" else "", str(keys), bgsave, aofrw,
               d.get("evicted_keys", ""), d.get("expired_keys", ""),
               f"{nc:.1f}" if nc != "" else "", f"{ims:.1f}"]
        f.write(",".join(row) + "\n")
        nxt += interval
        time.sleep(max(0.0, nxt - time.time()))
