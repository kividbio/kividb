#!/usr/bin/env python3
"""Idle CPU per key set alone: 5M EX 3600, then (flushed) 5M EX 1..3601 spread; also an empty server.
  idle2.py <kividb-binary> <io-model> <outdir>"""
import os, subprocess, sys, time, json
import redis
BIN, IO, OUT = sys.argv[1], sys.argv[2], sys.argv[3]
PORT = 7510
os.makedirs(OUT, exist_ok=True)
d = os.path.join(OUT, "data"); os.makedirs(d, exist_ok=True)
lf = open(os.path.join(OUT, "server.log"), "ab")
p = subprocess.Popen([BIN, "--port", str(PORT), "--io-model", IO, "--dir", d, "--loglevel", "notice"], cwd=d, stdout=lf, stderr=lf)
r = redis.Redis(port=PORT, socket_timeout=120)
for _ in range(100):
    try:
        r.ping(); break
    except redis.ConnectionError:
        time.sleep(0.1)
r.config_set("save", "")
HZ = os.sysconf("SC_CLK_TCK")
def ticks():
    f = open(f"/proc/{p.pid}/stat").read().rsplit(")", 1)[1].split()
    return int(f[11]) + int(f[12])
def cpu(secs):
    t0, k0 = time.time(), ticks(); time.sleep(secs); return round(100.0 * (ticks() - k0) / HZ / (time.time() - t0), 3)
def load(prefix, n, ttl_of):
    gen = (f"awk 'BEGIN{{for(i=0;i<{n};i++){{k=\"{prefix}:\" i; t={ttl_of}; "
           f"printf \"*5\\r\\n$3\\r\\nSET\\r\\n$%d\\r\\n%s\\r\\n$1\\r\\nv\\r\\n$2\\r\\nEX\\r\\n$%d\\r\\n%d\\r\\n\", length(k), k, length(t \"\"), t}}}}'"
           f" | redis-cli -p {PORT} --pipe")
    return subprocess.run(gen, shell=True, capture_output=True, text=True).stdout.strip().splitlines()[-1]
res = {"io": IO}
time.sleep(5); res["empty"] = cpu(60)
print("empty", res["empty"], flush=True)
print(load("a", 5_000_000, "3600"), flush=True); time.sleep(30); res["5M_ex3600"] = cpu(60); res["dbsize_a"] = r.dbsize()
print("5M EX 3600", res["5M_ex3600"], flush=True)
r.flushall(); time.sleep(5)
print(load("b", 5_000_000, "1 + (i * 7919) % 3601"), flush=True); time.sleep(30); res["5M_spread"] = cpu(60); res["dbsize_b"] = r.dbsize()
print("5M spread", res["5M_spread"], flush=True)
p.kill(); p.wait()
json.dump(res, open(os.path.join(OUT, "result.json"), "w"))
print("RESULT", json.dumps(res)); print("IDLE2-DONE", flush=True)
