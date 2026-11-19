#!/usr/bin/env python3
"""#879 cross-version matrix: v1.2.2 release binary against this build.
Run on the VM with ~/venv/bin/python. Prints PASS/FAIL lines."""
import hashlib
import os
import shutil
import subprocess
import time

import redis

HOME = os.path.expanduser("~")
V122 = HOME + "/v122/kividb/kividb"
NEW = HOME + "/t-feat/release/kividb"
BASE = "/dev/shm/compat879"
results = []


def check(name, ok, detail=""):
    results.append((name, ok))
    print(("PASS " if ok else "FAIL ") + name + ("  :: " + str(detail) if detail else ""), flush=True)


def start(binary, d, port, extra=(), conf=""):
    os.makedirs(d, exist_ok=True)
    with open(d + "/kividb.conf", "w") as f:
        f.write("enable-debug-command yes\n" + conf)
    log = open(d + "/server.log", "a")
    return subprocess.Popen([binary, "--port", str(port), "--bind", "127.0.0.1", "--dir", d,
                             "--configfile", d + "/kividb.conf", "--update-check", "no", *extra],
                            stdout=log, stderr=subprocess.STDOUT)


def cli(port):
    return redis.Redis(port=port, socket_timeout=60)


def ready(port, p, timeout=60):
    t0 = time.time()
    while time.time() - t0 < timeout:
        if p.poll() is not None:
            return False
        try:
            cli(port).ping()
            return True
        except Exception:
            time.sleep(0.05)
    return False


def stop(p, port):
    try:
        cli(port).execute_command("SHUTDOWN", "NOSAVE")
    except Exception:
        pass
    try:
        p.wait(20)
    except subprocess.TimeoutExpired:
        p.kill()
        p.wait()


def fresh(name):
    d = BASE + "/" + name
    shutil.rmtree(d, ignore_errors=True)
    os.makedirs(d)
    return d


def populate(c):
    c.execute_command("DEBUG", "POPULATE", 100000, "k", 100)
    c.set("ttl", "v", ex=100000)
    c.hset("h", mapping={"a": "1", "b": "2"})
    c.execute_command("HPEXPIRE", "h", 86400000, "FIELDS", 1, "a")
    c.rpush("l", *range(300))
    c.sadd("s", *range(300))
    c.zadd("z", {"m%d" % i: i for i in range(300)})
    c.xadd("st", {"f": "v"})
    c.execute_command("TS.ADD", "ts", 1000, 1)
    c.execute_command("JSON.SET", "j", "$", '{"a":1}')


def value_of(c, k):
    t = c.type(k)
    if t == b"string":
        return c.get(k)
    if t == b"hash":
        return sorted(c.hgetall(k).items())
    if t == b"list":
        return c.lrange(k, 0, -1)
    if t == b"set":
        return sorted(c.smembers(k))
    if t == b"zset":
        return c.zrange(k, 0, -1, withscores=True)
    if t == b"stream":
        return c.xrange(k)
    if t == b"TSDB-TYPE":
        return c.execute_command("TS.RANGE", k, "-", "+")
    if t == b"ReJSON-RL":
        return c.execute_command("JSON.GET", k)
    return t


def digest(c):
    keys = sorted(c.scan_iter(count=1000))
    h = hashlib.sha256()
    for k in keys:
        h.update(k)
        h.update(repr(value_of(c, k)).encode())
        h.update(b"ttl" if c.pttl(k) > 0 else b"-")
    return len(keys), h.hexdigest(), c.ttl("ttl")


def save_with(binary, name, conf="", port=7300):
    d = fresh(name)
    p = start(binary, d, port, conf=conf)
    assert ready(port, p)
    c = cli(port)
    populate(c)
    want = digest(c)
    c.save()
    stop(p, port)
    return d, want


def load_with(binary, d, port=7301):
    p = start(binary, d, port)
    ok = ready(port, p)
    got = digest(cli(port)) if ok else None
    if ok:
        stop(p, port)
    else:
        p.wait()
    return ok, got, p.returncode


def files(d):
    out = {}
    for n in sorted(os.listdir(d)):
        if n in ("server.log",):
            continue
        with open(os.path.join(d, n), "rb") as f:
            out[n] = hashlib.md5(f.read()).hexdigest()
    return out


def magic(path):
    with open(path, "rb") as f:
        return f.read(8)


# 1. a 1.2.2 file loads in this build
d, want = save_with(V122, "t1")
ok, got, _ = load_with(NEW, d)
check("1.2.2 dump.kdb loads in this build", ok and got[:2] == want[:2] and got[2] > 0, (want[:2], got))

# 2. this build with rdbcompression no: 1.2.2 loads it
d, want = save_with(NEW, "t2")
check("the default (rdbcompression no) writes KIVIDB01", magic(d + "/dump.kdb") == b"KIVIDB01")
ok, got, _ = load_with(V122, d)
check("the default dump loads in 1.2.2", ok and got[:2] == want[:2] and got[2] > 0, (want[:2], got))

# 3. the compressed file: 1.2.2 refuses with bad magic, exits non-zero, touches nothing
d, want = save_with(NEW, "t3", conf="rdbcompression yes\n")
check("rdbcompression yes writes KIVIDBZ1", magic(d + "/dump.kdb") == b"KIVIDBZ1")
before = files(d)
ok, got, rc = load_with(V122, d)
with open(d + "/server.log") as f:
    log = f.read()
check("1.2.2 refuses compressed dump: exits non-zero", (not ok) and rc not in (0, None), rc)
check("1.2.2 refuses compressed dump: says bad magic", "bad magic" in log, log.strip().splitlines()[-1:])
check("1.2.2 refuses compressed dump: directory unchanged", files(d) == before)
ok, got, _ = load_with(NEW, d)
check("this build loads its compressed dump", ok and got[:2] == want[:2], (want[:2], got))

# 4. kdb-decompress output loads in 1.2.2
out = d + "/plain.kdb"
r = subprocess.run([NEW, "kdb-decompress", d + "/dump.kdb", out], capture_output=True, text=True)
check("kdb-decompress succeeds", r.returncode == 0, r.stdout + r.stderr)
d4 = fresh("t4")
shutil.copy(out, d4 + "/dump.kdb")
ok, got, _ = load_with(V122, d4)
check("kdb-decompress output loads in 1.2.2", ok and got[:2] == want[:2], (want[:2], got))


def wait_sync(rport, n, timeout=60):
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            c = cli(rport)
            i = c.info("replication")
            if i.get("master_link_status") == "up" and c.dbsize() == n:
                return True
        except Exception:
            pass
        time.sleep(0.1)
    return False


def repl(master_bin, replica_bin, name, master_conf=""):
    dm, dr = fresh(name + "-m"), fresh(name + "-r")
    pm = start(master_bin, dm, 7310, conf=master_conf)
    assert ready(7310, pm)
    m = cli(7310)
    populate(m)
    n = m.dbsize()
    pr = start(replica_bin, dr, 7311, extra=("--replicaof", "127.0.0.1", "7310"))
    assert ready(7311, pr)
    synced = wait_sync(7311, n)
    m.set("after", "sync")
    time.sleep(1)
    follow = cli(7311).get("after") == b"sync"
    same = synced and digest(m)[:2] == digest(cli(7311))[:2]
    time.sleep(15)
    full = m.info("stats").get("sync_full")
    up = cli(7311).info("replication").get("master_link_status")
    stop(pr, 7311)
    stop(pm, 7310)
    with open(dm + "/server.log") as f:
        mlog = f.read()
    with open(dr + "/server.log") as f:
        rlog = f.read()
    return synced and follow and same, full, up, mlog, rlog


ok, full, up, mlog, rlog = repl(NEW, V122, "t5", master_conf="rdbcompression yes\n")
check("1.2.2 replica of this master: in sync, stream follows", ok)
check("1.2.2 replica of this master: one full sync, link up after 15 s", full == 1 and up == "up", (full, up))
check("1.2.2 replica of this master: sent plain", "a plain snapshot (the replica does not take kdb-blocks)" in mlog)

ok, full, up, mlog, rlog = repl(V122, NEW, "t6")
check("this replica of a 1.2.2 master: in sync, stream follows", ok)
check("this replica of a 1.2.2 master: one full sync, link up", full == 1 and up == "up", (full, up))

ok, full, up, mlog, rlog = repl(NEW, NEW, "t7", master_conf="rdbcompression yes\n")
check("this <-> this: in sync, stream follows", ok)
check("this <-> this: one full sync, link up", full == 1 and up == "up", (full, up))
check("this <-> this: master sent compressed", "a compressed snapshot (the replica takes kdb-blocks)" in mlog)
check("this <-> this: replica got compressed", "the master sent a compressed snapshot" in rlog)

ok, full, up, mlog, rlog = repl(NEW, NEW, "t8")
check("this <-> this, master with the default: in sync, plain", ok and "plain snapshot (rdbcompression no)" in mlog)

# 9. AOF directory: compressed base refused by 1.2.2; after rdbcompression no + rewrite, loaded
d = fresh("t9")
p = start(NEW, d, 7320, extra=("--appendonly", "yes", "--rdbcompression", "yes"))
assert ready(7320, p)
c = cli(7320)
populate(c)
c.bgrewriteaof()
time.sleep(3)
c.set("tail", "1")
want = digest(c)
stop(p, 7320)
base = [n for n in os.listdir(d) if n.endswith(".kdb")]
check("AOF base written compressed", all(magic(d + "/" + n) == b"KIVIDBZ1" for n in base) and base, base)
ok, got, rc = load_with(V122, d, port=7321)
check("1.2.2 refuses the compressed AOF base", not ok and rc not in (0, None), rc)
p = start(NEW, d, 7320, extra=("--appendonly", "yes", "--rdbcompression", "yes"))
assert ready(7320, p)
c = cli(7320)
check("this build restarts on compressed AOF base + log", digest(c)[:2] == want[:2])
c.config_set("rdbcompression", "no")
c.bgrewriteaof()
time.sleep(3)
stop(p, 7320)
check("AOF base plain after rdbcompression no + rewrite",
      all(magic(d + "/" + n) == b"KIVIDB01" for n in base))
p = start(V122, d, 7321, extra=("--aof",))  # 1.2.2 takes the flag, not the directive
ok = ready(7321, p)
got = digest(cli(7321)) if ok else None
if ok:
    stop(p, 7321)
check("1.2.2 starts on that AOF directory with every key", ok and got[:2] == want[:2], (want[:2], got))

# 10. DUMP payloads from this build RESTORE in 1.2.2
d1, d2 = fresh("t10a"), fresh("t10b")
pa, pb = start(NEW, d1, 7330), start(V122, d2, 7331)
assert ready(7330, pa) and ready(7331, pb)
a, b = cli(7330), cli(7331)
populate(a)
bad = []
for k in ["h", "l", "s", "z", "st", "ts", "j", "ttl", "k:5"]:
    payload = a.dump(k)
    try:
        b.restore(k, 0, payload, replace=True)
        # By value: DEBUG DIGEST-VALUE of a hash or a stream differs between
        # 1.2.2 and 1.3 for the same value.
        if value_of(a, k) != value_of(b, k):
            bad.append(k)
    except Exception as e:
        bad.append("%s:%s" % (k, e))
check("DUMP from this build RESTOREs in 1.2.2 with the same value", not bad, bad)
stop(pa, 7330)
stop(pb, 7331)

print("SUMMARY %d/%d passed" % (sum(1 for _, ok in results if ok), len(results)))
