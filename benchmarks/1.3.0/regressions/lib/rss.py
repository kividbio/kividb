"""Memory readings: a process's VmRSS and smaps split, and INFO memory.

    vmrss_mib(pid)                     VmRSS of pid, MiB
    smaps_mib(pid)                     {"heap": .., "anon": .., "file": ..} resident MiB
    info_memory(client)                INFO memory as a dict (redis-py client)
    used_memory_mib(client)            used_memory, MiB
    sample_at(pid, t0, marks, every)   VmRSS at each of `marks` seconds after t0, plus
                                       a timeline every `every` seconds, issuing no
                                       commands to the server (an idle server is what
                                       some give-back paths wait for)
"""
import time

MIB = 1048576.0


def vmrss_mib(pid):
    with open("/proc/%d/status" % pid) as f:
        for line in f:
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) / 1024.0
    raise RuntimeError("no VmRSS for pid %d" % pid)


def smaps_mib(pid):
    out = {"heap": 0, "anon": 0, "file": 0}
    cur = None
    with open("/proc/%d/smaps" % pid) as f:
        for line in f:
            parts = line.split()
            if len(parts) >= 5 and "-" in parts[0] and not line.startswith(" "):
                if parts[-1] == "[heap]":
                    cur = "heap"
                elif len(parts) == 5:
                    cur = "anon"
                else:
                    cur = "file"
            elif line.startswith("Rss:") and cur:
                out[cur] += int(parts[1])
    return {k: round(v / 1024.0, 1) for k, v in out.items()}


def info_memory(client):
    return client.info("memory")


def used_memory_mib(client):
    return info_memory(client).get("used_memory", 0) / MIB


def sample_at(pid, t0, marks, every=0.5):
    """Block until the last mark; return ({mark: rss}, [(t, rss), ...])."""
    marks = sorted(marks)
    at = {}
    timeline = []
    nxt_tick = t0
    for m in marks:
        while True:
            now = time.perf_counter()
            target = min(t0 + m, nxt_tick)
            if now < target:
                time.sleep(target - now)
                now = time.perf_counter()
            if now >= nxt_tick:
                timeline.append((round(now - t0, 2), round(vmrss_mib(pid), 1)))
                nxt_tick += every
            if now >= t0 + m:
                at[m] = round(vmrss_mib(pid), 1)
                break
    return at, timeline
