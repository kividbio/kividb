#!/usr/bin/env python3
"""#618 regression guard: idle CPU with many keys with a TTL, and a clock stepped back and forward.

  idle.py <kividb-binary> <io-model> <outdir>

Loads a:0..5M-1 (EX 3600), b:0..5M-1 (EX 1..3601 spread), c:0..1M-1 (EX 86400, must never expire),
measures the server's CPU (utime+stime from /proc) while idle, then:
  back:    clock -1 h, 60 s, clock +1 h, 60 s   (no key may expire that is not really due)
  forward: clock +1 h, 40 s, clock -1 h, 60 s   (keys due at the forward time may go; c:* must stay)
Before each step, every key's PEXPIRETIME is recorded; afterwards every key is checked: present, or
its recorded deadline is before the latest clock reading the server could have seen.
Needs chrony stopped (done here) and passwordless sudo.
"""
import array, json, os, subprocess, sys, time
import redis

BIN, IO, OUT = sys.argv[1], sys.argv[2], sys.argv[3]
PORT = 7500
N_A, N_B, N_C = 5_000_000, 5_000_000, 1_000_000
os.makedirs(OUT, exist_ok=True)
logf = open(os.path.join(OUT, "idle.log"), "a")
res = {"io": IO, "bin": BIN}


def say(*a):
    s = time.strftime("%H:%M:%S ") + " ".join(str(x) for x in a)
    print(s, flush=True); logf.write(s + "\n"); logf.flush()


def sh(cmd):
    return subprocess.run(cmd, shell=True, check=True, capture_output=True, text=True).stdout.strip()


d = os.path.join(OUT, "data"); os.makedirs(d, exist_ok=True)
srvlog = open(os.path.join(OUT, "server.log"), "ab")
p = subprocess.Popen([BIN, "--port", str(PORT), "--io-model", IO, "--dir", d, "--loglevel", "notice"],
                     cwd=d, stdout=srvlog, stderr=srvlog)
r = redis.Redis(port=PORT, socket_timeout=120)
for _ in range(100):
    try:
        r.ping(); break
    except redis.ConnectionError:
        time.sleep(0.1)
try:
    say("CONFIG SET save '':", r.config_set("save", ""), "now:", r.config_get("save"))
except Exception as e:
    say("CONFIG SET save failed:", e)
say("I/O model line:", sh(f"grep -a -i 'I/O model' {OUT}/server.log | head -1"))


def ticks():
    f = open(f"/proc/{p.pid}/stat").read().rsplit(")", 1)[1].split()
    return int(f[11]) + int(f[12])  # utime, stime (fields 14, 15)


HZ = os.sysconf("SC_CLK_TCK")


def cpu_over(secs, step=None):
    """% of one core over `secs`; with `step`, also each `step`-second slice."""
    t0, k0 = time.time(), ticks()
    slices = []
    if step:
        lt, lk = t0, k0
        while time.time() - t0 < secs - 0.01:
            time.sleep(step)
            t, k = time.time(), ticks()
            slices.append(round(100.0 * (k - lk) / HZ / (t - lt), 2)); lt, lk = t, k
    else:
        time.sleep(secs)
    t1, k1 = time.time(), ticks()
    return round(100.0 * (k1 - k0) / HZ / (t1 - t0), 3), slices


def load(prefix, n, ttl_of):
    gen = (f"awk 'BEGIN{{for(i=0;i<{n};i++){{k=\"{prefix}:\" i; t={ttl_of}; "
           f"printf \"*5\\r\\n$3\\r\\nSET\\r\\n$%d\\r\\n%s\\r\\n$1\\r\\nv\\r\\n$2\\r\\nEX\\r\\n$%d\\r\\n%d\\r\\n\", length(k), k, length(t \"\"), t}}}}'"
           f" | redis-cli -p {PORT} --pipe")
    return sh(gen).splitlines()[-1]


t = time.time()
say("load a", load("a", N_A, "3600"))
say("load b", load("b", N_B, "1 + (i * 7919) % 3601"))
say("load c", load("c", N_C, "86400"))
say(f"loaded in {time.time()-t:.1f}s dbsize={r.dbsize()}")
res["loaded_dbsize"] = r.dbsize()
say("settle 30 s (a sweep of every shard)"); time.sleep(30)
res["idle_60s_pct"], _ = cpu_over(60)
say("idle CPU over 60 s:", res["idle_60s_pct"], "% of a core")


def info_stats():
    s = r.info("stats")
    return {k: s.get(k) for k in ("expired_keys", "expired_keys_active", "evicted_keys")}


def deadlines():
    """PEXPIRETIME of every key, by prefix; -2 for gone."""
    out = {}
    for prefix, n in (("a", N_A), ("b", N_B), ("c", N_C)):
        arr = array.array("q")
        for lo in range(0, n, 20000):
            pipe = r.pipeline(transaction=False)
            for i in range(lo, min(n, lo + 20000)):
                pipe.execute_command("PEXPIRETIME", f"{prefix}:{i}")
            arr.extend(pipe.execute())
        out[prefix] = arr
    return out


def check(before, horizon_ms, label):
    """Every key that had a deadline at or after horizon_ms must still be there, with the same deadline."""
    now = deadlines()
    # Keys may come due while the dump runs: the horizon is the latest clock the server can have read.
    horizon_ms = max(horizon_ms, now_ms() + 50)
    gone_early, gone_ok, changed, present = 0, 0, 0, 0
    examples = []
    for prefix in before:
        b, a = before[prefix], now[prefix]
        for i in range(len(b)):
            if b[i] == -2:
                continue
            if a[i] == -2:
                if b[i] >= horizon_ms:
                    gone_early += 1
                    if len(examples) < 10: examples.append(f"{prefix}:{i} deadline {b[i]} >= {horizon_ms}")
                else:
                    gone_ok += 1
            else:
                present += 1
                if a[i] != b[i]:
                    changed += 1
                    if len(examples) < 10: examples.append(f"{prefix}:{i} pexpiretime {b[i]} -> {a[i]}")
    out = {"horizon_ms": horizon_ms, "present": present, "gone_due": gone_ok, "gone_EARLY": gone_early, "deadline_changed": changed, "examples": examples}
    say(label, json.dumps(out))
    return out, now


def now_ms():
    return int(time.time() * 1000)


def step(secs):
    sh(f"sudo date -s @$(python3 -c 'import time;print(f\"{{time.time()+({secs}):.3f}}\")') >/dev/null")


sh("sudo systemctl stop chrony 2>/dev/null || sudo systemctl stop systemd-timesyncd 2>/dev/null || true")
try:
    # ---- back 1 h, then forward to real time
    before = deadlines()
    s0 = info_stats(); db0 = r.dbsize(); real0 = now_ms()
    step(-3600); say("clock stepped back 1 h:", sh("date -u"))
    back_cpu, back_slices = cpu_over(60, 5)
    say("CPU while back (60 s):", back_cpu, back_slices)
    step(+3600); say("clock stepped forward to real time:", sh("date -u"))
    fwd_cpu, fwd_slices = cpu_over(60, 5)
    say("CPU after return (60 s):", fwd_cpu, fwd_slices)
    s1 = info_stats(); db1 = r.dbsize()
    # The latest clock the server saw is now (the back period's clock was earlier).
    chk, _ = check(before, now_ms() - 0, "back-check")
    res["back"] = {"cpu_back_pct": back_cpu, "slices_back": back_slices, "cpu_after_pct": fwd_cpu,
                   "slices_after": fwd_slices, "dbsize": [db0, db1], "stats": [s0, s1],
                   "expired_delta": s1["expired_keys"] - s0["expired_keys"], "check": chk}

    # ---- forward 1 h, then back
    before = deadlines()
    s0 = info_stats(); db0 = r.dbsize()
    step(+3600); say("clock stepped forward 1 h:", sh("date -u"))
    fwd_cpu, fwd_slices = cpu_over(40, 5)
    say("CPU while forward (40 s):", fwd_cpu, fwd_slices)
    t_max = now_ms()  # the latest clock the server can have read
    s_mid = info_stats(); db_mid = r.dbsize()
    step(-3600); say("clock stepped back to real time:", sh("date -u"))
    back_cpu, back_slices = cpu_over(60, 5)
    say("CPU after return (60 s):", back_cpu, back_slices)
    s1 = info_stats(); db1 = r.dbsize()
    chk, after = check(before, t_max + 50, "forward-check")
    # How many keys were due at t_max, by the recorded deadlines.
    due = sum(1 for pr in before for x in before[pr] if 0 <= x < t_max)
    res["forward"] = {"cpu_forward_pct": fwd_cpu, "slices_forward": fwd_slices, "cpu_after_pct": back_cpu,
                      "slices_after": back_slices, "dbsize": [db0, db_mid, db1], "stats": [s0, s_mid, s1],
                      "expired_delta": s1["expired_keys"] - s0["expired_keys"], "due_at_forward_clock": due,
                      "c_keys_present": sum(1 for x in after["c"] if x != -2), "check": chk}
    say("idle 60 s at the end"); res["idle_end_60s_pct"], _ = cpu_over(60)
    say("idle CPU at the end:", res["idle_end_60s_pct"])
finally:
    sh("sudo systemctl start chrony 2>/dev/null || sudo systemctl start systemd-timesyncd 2>/dev/null || true")
    sh("sudo chronyc makestep 2>/dev/null || true")
    p.kill(); p.wait()
    json.dump(res, open(os.path.join(OUT, "result.json"), "w"), indent=1)
    say("RESULT", json.dumps(res))
    say("IDLE-DONE")
