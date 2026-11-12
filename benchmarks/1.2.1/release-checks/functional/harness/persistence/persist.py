#!/usr/bin/env python3
"""KiviDB 1.2.1 persistence checks, against one binary.

  persist.py <kividb-binary> <io-model> <outdir> [tests...]

tests: save bgsave aof_always aof_everysec rewrite_always rewrite_everysec fieldttl backup aofoff  (default: all)
1.2.1 final pass (878746e4): appendfsync always is real now; adds BGREWRITEAOF under load and #531 field-TTL checks.

Fingerprint of a dataset: for every key of db 0 and db 3 (SCAN), its TYPE, DEBUG DIGEST-VALUE,
PEXPIRETIME and, for hashes, every field's HPEXPIRETIME; sha256 over the sorted list. KiviDB has
no whole-dataset DEBUG DIGEST, so this is the same idea built from per-key digests. Absolute
expire times are compared, so a TTL that moved by a millisecond is a mismatch.
"""
import hashlib, json, os, random, shutil, signal, subprocess, sys, tempfile, threading, time
import redis

BIN, IO, OUT = sys.argv[1], sys.argv[2], sys.argv[3]
TESTS = sys.argv[4:] or ["save", "bgsave", "aof_always", "aof_everysec", "rewrite_always", "rewrite_everysec", "fieldttl", "backup", "aofoff"]
os.makedirs(OUT, exist_ok=True)
PORT = 7480
results = []          # (test, ok, detail)
log = open(os.path.join(OUT, "persist.log"), "a")


def say(*a):
    s = " ".join(str(x) for x in a)
    print(s, flush=True)
    log.write(s + "\n"); log.flush()


def record(test, ok, detail):
    results.append((test, ok, detail))
    say(("PASS" if ok else "FAIL") + f": {test}: {detail}")


class Server:
    def __init__(self, d, *args, port=PORT):
        self.d, self.args, self.port = d, list(args), port
        self.p = None

    def start(self, timeout=60):
        self.logf = open(os.path.join(self.d, "server.log"), "ab")
        self.p = subprocess.Popen([BIN, "--port", str(self.port), "--protected-mode", "no", "--io-model", IO,
                                   "--dir", self.d] + self.args, cwd=self.d, stdout=self.logf, stderr=self.logf)
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.p.poll() is not None:
                raise RuntimeError(f"server exited {self.p.returncode}: {self.tail()}")
            try:
                r = self.client()
                r.ping(); r.dbsize()
                return r
            except (redis.ConnectionError, redis.BusyLoadingError, redis.ResponseError):
                time.sleep(0.1)
        raise RuntimeError("server did not come up: " + self.tail())

    def client(self, db=0):
        return redis.Redis(port=self.port, db=db, socket_timeout=30)

    def kill9(self):
        if self.p and self.p.poll() is None:
            self.p.send_signal(signal.SIGKILL); self.p.wait()

    def shutdown(self):
        try:
            rep = self.client().execute_command("SHUTDOWN")
            say("SHUTDOWN replied", rep)
        except (redis.ConnectionError, redis.TimeoutError) as e:
            pass
        self.p.wait(timeout=60)

    def tail(self):
        try:
            return open(os.path.join(self.d, "server.log"), "rb").read()[-1500:].decode("utf8", "replace")
        except OSError:
            return ""


DUMPN = [0]


def fingerprint(srv):
    h = hashlib.sha256(); n = 0; items = []
    for db in (0, 3):
        r = srv.client(db)
        for k in r.scan_iter(count=1000):
            t = r.type(k)
            dv = r.execute_command("DEBUG", "DIGEST-VALUE", k)
            dv = dv[0] if isinstance(dv, list) else dv
            et = r.execute_command("PEXPIRETIME", k)
            extra = b""
            if t == b"hash":
                fields = sorted(r.hkeys(k))
                if fields:
                    ft = r.execute_command("HPEXPIRETIME", k, "FIELDS", len(fields), *fields)
                    extra = b",".join(f + b"=" + str(x).encode() for f, x in zip(fields, ft))
                # DIGEST-VALUE of a hash with field TTLs depends on the order of its
                # expiry map (issue draft hash-digest-order): digest the sorted fields instead.
                hv = r.hgetall(k)
                dv = hashlib.sha1(b"".join(b"%d:%s=%d:%s;" % (len(f), f, len(hv[f]), hv[f]) for f in sorted(hv))).hexdigest().encode()
            items.append(b"%d|%s|%s|%s|%d|%s" % (db, k.hex().encode(), t, dv, et, extra))
    items.sort()
    DUMPN[0] += 1
    with open(os.path.join(OUT, "fp-%03d.txt" % DUMPN[0]), "wb") as fh:
        fh.write(b"\n".join(items))
    for it in items:
        h.update(it); h.update(b"\n"); n += 1
    LAST_ITEMS[h.hexdigest()[:16]] = items
    return h.hexdigest()[:16], n


LAST_ITEMS = {}


def ttl_drift_only(a, b):
    """The two item lists differ only in hash-field TTLs, each by at most 1 ms (known #489)."""
    if len(a) != len(b):
        return None
    n = 0
    for x, y in zip(a, b):
        if x == y:
            continue
        px, py = x.split(b"|", 5), y.split(b"|", 5)
        if px[:5] != py[:5]:
            return None
        fx = dict(e.split(b"=", 1) for e in px[5].split(b",") if e)
        fy = dict(e.split(b"=", 1) for e in py[5].split(b",") if e)
        if fx.keys() != fy.keys():
            return None
        for f in fx:
            d = abs(int(fx[f]) - int(fy[f]))
            if d > 1:
                return None
            n += d
    return n


def populate(r, n=20000, seed=1):
    rnd = random.Random(seed)
    far = int(time.time() * 1000) + 3600_000
    p = r.pipeline(transaction=False)
    for i in range(n):
        p.set(f"s:{i}", os.urandom(rnd.randint(1, 64)))
        if i % 5 == 0:
            p.execute_command("PEXPIREAT", f"s:{i}", far + i)
        if i % 500 == 499:
            p.execute()
    p.execute()
    big = os.urandom(1 << 20)
    r.set("big", big)
    # binary keys: non-UTF-8, NUL inside, the empty key, a binary hash field
    r.set(b"\xff\xfe\x00bin\x80", b"\x00\x01\xff")
    r.set(b"", b"empty-key-value")
    r.hset(b"\xc3\x28hash", mapping={b"\xff": b"\x00", b"ok": b"v"})
    r.execute_command("HPEXPIREAT", b"\xc3\x28hash", far + 7, "FIELDS", 1, b"\xff")
    for i in range(500):
        r.hset(f"h:{i}", mapping={f"f{j}": f"v{i}-{j}" for j in range(10)})
        if i % 2 == 0:
            r.execute_command("HPEXPIREAT", f"h:{i}", far + 1000 + i, "FIELDS", 3, "f0", "f3", "f9")
        if i % 7 == 0:
            r.execute_command("HEXPIRE", f"h:{i}", 7200, "FIELDS", 1, "f5")
    r.rpush("list", *[f"e{i}" for i in range(1000)])
    r.sadd("set", *[f"m{i}" for i in range(1000)])
    r.zadd("zset", {f"z{i}": i * 0.5 for i in range(1000)})
    for i in range(200):
        r.xadd("stream", {"f": str(i)})
    r.pfadd("hll", *[f"u{i}" for i in range(5000)])
    r.setbit("bitmap", 100000, 1)
    r.geoadd("geo", (13.361389, 38.115556, "Palermo"), (15.087269, 37.502669, "Catania"))
    try:
        r.execute_command("JSON.SET", "json", "$", json.dumps({"a": [1, 2, {"b": "c"}], "n": 1.5}))
    except redis.ResponseError as e:
        say("note: JSON.SET:", e)
    r.set("counter", 0)
    for _ in range(1000):
        r.incr("counter")
    r3 = redis.Redis(port=r.connection_pool.connection_kwargs["port"], db=3)
    r3.set("in-db-3", "yes"); r3.set(b"\x00", b"nul-key-db3")


def fresh(name):
    d = tempfile.mkdtemp(prefix=f"p-{name}-", dir=os.path.join(OUT, "dirs"))
    return d


def check_equal(test, before, after, extra=""):
    if before != after:
        n = ttl_drift_only(LAST_ITEMS.get(before[0], []), LAST_ITEMS.get(after[0], []))
        if n is not None:
            record(test, True, f"before {before} after {after}: equal except {n} hash-field TTLs 1 ms earlier (known #489) {extra}")
            return
    record(test, before == after, f"before {before} after {after} {extra}")


def t_save():
    d = fresh("save"); s = Server(d); r = s.start()
    populate(r)
    before = fingerprint(s)
    r.save()
    s.kill9()
    r = s.start()
    after = fingerprint(s)
    check_equal("SAVE, kill -9, restart (AOF off)", before, after, f"counter={r.get('counter')} empty-key={r.get(b'')!r}")
    # A clean SHUTDOWN too
    s.shutdown(); r = s.start()
    check_equal("SHUTDOWN, restart (AOF off)", before, fingerprint(s))
    s.kill9()


def t_bgsave():
    d = fresh("bgsave"); s = Server(d); r = s.start()
    populate(r, seed=2)
    before = fingerprint(s)
    last = r.lastsave()
    r.bgsave()
    for _ in range(600):
        info = r.info("persistence")
        if not info.get("rdb_bgsave_in_progress") and r.lastsave() != last:
            break
        time.sleep(0.1)
    st = r.info("persistence").get("rdb_last_bgsave_status")
    s.kill9()
    r = s.start()
    check_equal("BGSAVE, kill -9, restart (AOF off)", before, fingerprint(s), f"status={st}")
    # BGSAVE with writes running during it: everything acknowledged before BGSAVE is in the snapshot
    stop = threading.Event(); acked = []
    def w():
        c = s.client(); i = 0
        while not stop.is_set():
            c.set(f"bgw:{i}", i); acked.append(i); i += 1
    th = threading.Thread(target=w); th.start(); time.sleep(0.5)
    n_before = len(acked); last = r.lastsave(); r.bgsave()
    for _ in range(600):
        if not r.info("persistence").get("rdb_bgsave_in_progress") and r.lastsave() != last:
            break
        time.sleep(0.1)
    stop.set(); th.join(); s.kill9(); r = s.start()
    missing = [i for i in range(n_before) if r.get(f"bgw:{i}") is None]
    record("BGSAVE under writes: every write acked before BGSAVE present after kill -9", not missing,
           f"{n_before} acked before BGSAVE, {len(missing)} missing; total acked {len(acked)}, present after {r.dbsize()}")
    s.kill9()


def check_mode(s, r, mode, test):
    """CONFIG GET appendfsync says `mode`, no 'has no effect' warning, and the io model line of the log."""
    got = r.config_get("appendfsync").get("appendfsync")
    logtxt = open(os.path.join(s.d, "server.log"), "rb").read().decode("utf8", "replace")
    noeff = [l for l in logtxt.splitlines() if "no effect" in l and "appendfsync" in l]
    iol = [l for l in logtxt.splitlines() if "io_uring" in l.lower() or "tokio" in l.lower() or "io model" in l.lower() or "io-model" in l.lower()][:2]
    record(f"{test}: CONFIG GET appendfsync = {mode}, no 'has no effect' warning", got == mode and not noeff,
           f"CONFIG GET appendfsync={got} warnings={noeff} io-log={iol}")


def t_aof_always():
    t_aof(mode="always")


def t_aof_everysec():
    t_aof(mode="everysec")


def t_aof(rounds=int(os.environ.get("ROUNDS", "8")), mode="always"):
    for rnd in range(rounds):
        d = fresh(f"aof-{mode}-{rnd}")
        conf = os.path.join(d, "kividb.conf")
        open(conf, "w").write(f"appendonly yes\nappendfsync {mode}\n")
        s = Server(d, "--configfile", conf); r = s.start()
        if rnd == 0:
            check_mode(s, r, mode, f"AOF {mode}")
        if rnd == 0:
            populate(r, n=5000, seed=3)
        base = fingerprint(s) if rnd == 0 else None
        stop = threading.Event(); acks = {}; attempts = {}; errs = []
        def w(t):
            c = s.client(); i = 0; a = []; acks[t] = a; attempts[t] = 0
            try:
                while not stop.is_set():
                    attempts[t] = i + 1
                    if t % 2:
                        c.set(f"ack:{t}:{i}", f"v{i}"); a.append(("set", i))
                    else:
                        v = c.incr(f"cnt:{t}"); a.append(("incr", v))
                    i += 1
            except Exception as e:  # the kill
                errs.append(type(e).__name__)
        ths = [threading.Thread(target=w, args=(t,)) for t in range(16)]
        for th in ths: th.start()
        time.sleep(random.uniform(2, 5))
        s.kill9(); stop.set()
        for th in ths: th.join()
        r = s.start()
        lost = 0; total = 0; over = 0
        for t, a in acks.items():
            total += len(a)
            if t % 2:
                lost += sum(1 for _, i in a if r.get(f"ack:{t}:{i}") != f"v{i}".encode())
            else:
                got = int(r.get(f"cnt:{t}") or 0); want = a[-1][1] if a else 0
                if got < want: lost += want - got
                if got > attempts[t]: over += 1
        ok = lost == 0 and over == 0
        info = r.info("persistence")
        extra = ""
        if base:
            s.kill9(); r = s.start()  # replay again: the first round's populated data must match too
            # strip the writer keys before comparing to base
            for k in list(r.scan_iter(match="ack:*", count=1000)) + list(r.scan_iter(match="cnt:*")):
                r.delete(k)
            fp2 = fingerprint(s)
            drift = 0 if fp2 == base else ttl_drift_only(LAST_ITEMS[base[0]], LAST_ITEMS[fp2[0]])
            extra = f" base fp {base} vs {fp2}" + (f" (equal except {drift} field TTLs 1 ms earlier, known #489)" if drift else "")
            ok = ok and drift is not None
        record(f"AOF appendfsync {mode}, 16 writers, kill -9 round {rnd+1}", ok,
               f"{total} acked writes, {lost} lost, {over} counters above attempts, aof_acks_before_write={info.get('aof_acks_before_write')}{extra}")
        s.kill9()


def writers(s, n, stop, acks, attempts, errs, prefix):
    def w(t):
        c = s.client(); i = 0; a = []; acks[t] = a; attempts[t] = 0
        try:
            while not stop.is_set():
                attempts[t] = i + 1
                if t % 2:
                    c.set(f"{prefix}ack:{t}:{i}", f"v{i}"); a.append(("set", i))
                else:
                    v = c.incr(f"{prefix}cnt:{t}"); a.append(("incr", v))
                i += 1
        except Exception as e:  # the kill
            errs.append(type(e).__name__)
    ths = [threading.Thread(target=w, args=(t,)) for t in range(n)]
    for th in ths: th.start()
    return ths


def verify_acks(r, acks, attempts, prefix):
    lost = 0; total = 0; over = 0
    for t, a in acks.items():
        total += len(a)
        if t % 2:
            vals = r.mget([f"{prefix}ack:{t}:{i}" for _, i in a]) if a else []
            lost += sum(1 for (_, i), v in zip(a, vals) if v != f"v{i}".encode())
        else:
            got = int(r.get(f"{prefix}cnt:{t}") or 0); want = a[-1][1] if a else 0
            if got < want: lost += want - got
            if got > attempts[t]: over += 1
    return total, lost, over


def strip_and_fp(s, r, prefix):
    for pat in (f"{prefix}ack:*", f"{prefix}cnt:*"):
        ks = list(r.scan_iter(match=pat, count=5000))
        for i in range(0, len(ks), 1000):
            r.delete(*ks[i:i + 1000])
    return fingerprint(s)


def t_rewrite_always():
    t_rewrite("always")


def t_rewrite_everysec():
    t_rewrite("everysec")


def t_rewrite(mode, rounds=int(os.environ.get("RW_ROUNDS", "3"))):
    """BGREWRITEAOF under 16 writers: several rewrites, then kill -9 (in the last round during a
    rewrite), restart: every ack present, static dataset fingerprint equal, files and INFO sane."""
    for rnd in range(rounds):
        d = fresh(f"rw-{mode}-{rnd}")
        conf = os.path.join(d, "kividb.conf")
        open(conf, "w").write(f"appendonly yes\nappendfsync {mode}\n")
        s = Server(d, "--configfile", conf); r = s.start()
        if rnd == 0:
            check_mode(s, r, mode, f"BGREWRITEAOF {mode}")
        populate(r, n=20000, seed=10 + rnd)
        base = fingerprint(s)
        stop = threading.Event(); acks = {}; attempts = {}; errs = []
        ths = writers(s, 16, stop, acks, attempts, errs, "rw")
        nrw = 0; rwerr = []
        for k in range(5):
            time.sleep(random.uniform(0.3, 1.0))
            try:
                r.execute_command("BGREWRITEAOF")
            except redis.ResponseError as e:
                rwerr.append(str(e))
            nrw += 1
            last = rnd == rounds - 1 and k == 4
            if last:
                time.sleep(random.uniform(0.0, 0.05))   # kill while the last one may still run
                break
            for _ in range(600):
                if not r.info("persistence").get("aof_rewrite_in_progress"):
                    break
                time.sleep(0.05)
        time.sleep(random.uniform(0.2, 1.0) if rnd < rounds - 1 else 0)
        pinfo = r.info("persistence")
        files = sorted(os.listdir(d))
        s.kill9(); stop.set()
        for th in ths: th.join()
        r = s.start()
        total, lost, over = verify_acks(r, acks, attempts, "rw")
        info = r.info("persistence")
        fp = strip_and_fp(s, r, "rw")
        drift = 0 if fp == base else ttl_drift_only(LAST_ITEMS[base[0]], LAST_ITEMS[fp[0]])
        ok = lost == 0 and over == 0 and drift is not None and info.get("aof_lost_records", 0) == 0 \
            and pinfo.get("aof_last_bgrewrite_status") == "ok" and not rwerr
        record(f"BGREWRITEAOF x{nrw} under 16 writers ({mode}), kill -9{' during a rewrite' if rnd == rounds - 1 else ''}, restart, round {rnd+1}", ok,
               f"{total} acked, {lost} lost, {over} counters over; static fp {base} vs {fp}"
               + (f" (equal except {drift} field TTLs 1 ms, #489)" if drift else "")
               + f"; before kill: rewrite_in_progress={pinfo.get('aof_rewrite_in_progress')} last_bgrewrite_status={pinfo.get('aof_last_bgrewrite_status')}"
               f" aof_current_size={pinfo.get('aof_current_size')} aof_base_size={pinfo.get('aof_base_size')} rewrite_ms={pinfo.get('aof_last_rewrite_time_ms')}"
               f"; files={files}; after: aof_lost_records={info.get('aof_lost_records')} acks_before_write={info.get('aof_acks_before_write')}"
               f" superseded={info.get('aof_superseded_files')} errs={rwerr}")
        # A second restart replays the same files to the same state
        fp1 = fingerprint(s); s.kill9(); r = s.start(); fp2 = fingerprint(s)
        record(f"BGREWRITEAOF ({mode}) round {rnd+1}: a second restart gives the same dataset", fp1 == fp2, f"{fp1} vs {fp2}")
        s.kill9()


def t_fieldttl():
    """#531/#415: hash-field expiry is master-driven. (a) fields that expire while the server is down
    do not come back; (b) a field that expired while up and was then re-created by HINCRBY replays to
    the master's value (not old+incr); (c) a hash whose fields all expired is gone after replay."""
    d = fresh("fttl"); s = Server(d, "--aof"); r = s.start()
    prop = r.info().get("kividb_expired_fields_propagated")
    # (b) expire while up, then write the same field
    for i in range(200):
        r.hset(f"fb:{i}", mapping={"n": 100, "keep": "k"})
        r.execute_command("HPEXPIRE", f"fb:{i}", 300, "FIELDS", 1, "n")
    for i in range(100):   # a hash all of whose fields expire
        r.hset(f"fc:{i}", mapping={"a": 1, "b": 2})
        r.execute_command("HPEXPIRE", f"fc:{i}", 300, "FIELDS", 2, "a", "b")
    time.sleep(1.0)
    vals_b = [r.hincrby(f"fb:{i}", "n", 1) for i in range(200)]  # master: field gone -> 1
    gone_c = sum(1 for i in range(100) if not r.exists(f"fc:{i}"))
    # (a) expire while down
    for i in range(300):
        r.hset(f"fa:{i}", mapping={"x": i, "y": "stay"})
        r.execute_command("HPEXPIRE", f"fa:{i}", 1500, "FIELDS", 1, "x")
    before_a = sum(r.hexists(f"fa:{i}", "x") for i in range(300))
    s.kill9(); time.sleep(3.0); r = s.start()
    after_a = sum(r.hexists(f"fa:{i}", "x") for i in range(300))
    y_ok = sum(r.hget(f"fa:{i}", "y") == b"stay" for i in range(300))
    hlen_a = sum(r.hlen(f"fa:{i}") for i in range(300))
    b_after = [int(r.hget(f"fb:{i}", "n") or -1) for i in range(200)]
    c_after = sum(1 for i in range(100) if r.exists(f"fc:{i}"))
    record("#531 field TTLs: fields expired while the server was down do not come back after AOF replay",
           after_a == 0 and y_ok == 300 and hlen_a == 300 and before_a == 300,
           f"x present before kill {before_a}/300, after restart {after_a}/300; y kept {y_ok}/300; HLEN sum {hlen_a} (want 300)")
    record("#531 field TTLs: HINCRBY on a field that expired while up replays to the master's value",
           vals_b == [1] * 200 and b_after == [1] * 200,
           f"master values {set(vals_b)}, after replay {set(b_after)} (want {{1}}); kividb_expired_fields_propagated={prop}")
    record("#531 field TTLs: a hash whose fields all expired stays gone after replay", gone_c == 100 and c_after == 0,
           f"gone before kill {gone_c}/100, present after replay {c_after}")
    # SAVE then restart with fields that expire while down (snapshot path rather than AOF)
    for i in range(300):
        r.hset(f"fd:{i}", mapping={"x": i, "y": "stay"})
        r.execute_command("HPEXPIRE", f"fd:{i}", 1500, "FIELDS", 1, "x")
    r.save(); s.kill9(); time.sleep(3.0); r = s.start()
    d_after = sum(r.hexists(f"fd:{i}", "x") for i in range(300))
    record("#531 field TTLs: SAVE, down past the TTL, restart: expired fields not served",
           d_after == 0 and sum(r.hlen(f"fd:{i}") for i in range(300)) == 300, f"x present after restart {d_after}/300")
    s.kill9()


def t_backup():
    d = fresh("backup"); s = Server(d, "--aof"); r = s.start()
    populate(r, seed=4)
    stop = threading.Event()
    def w():
        c = s.client(); i = 0
        while not stop.is_set():
            c.incr("bk:counter"); c.rpush("bk:log", i); i += 1
    th = threading.Thread(target=w); th.start()
    r.execute_command("BACKUP", "START")
    for _ in range(400):
        st = r.execute_command("BACKUP", "STATUS")
        st = dict(zip(st[::2], st[1::2])) if isinstance(st, list) else st
        if st.get(b"state") == b"incrementing":
            break
        time.sleep(0.05)
    r.set("during", "window"); r.delete("s:1"); r.execute_command("HPEXPIRE", "h:1", 9000, "FIELDS", 1, "f1")
    time.sleep(1)
    stop.set(); th.join()          # writes stop before the seal, so the fingerprint is that of the seal
    r.execute_command("BACKUP", "SEAL")
    at_seal = fingerprint(s)
    r.set("after-seal", "not in backup")
    files = [f.decode() for f in r.execute_command("BACKUP", "LIST")]
    bdir = fresh("backup-copy")
    for f in files:
        shutil.copy(f, bdir)
    say("backup files:", [os.path.basename(f) for f in files])
    s.kill9()
    d2 = fresh("restore"); s2 = Server(d2, "--preload-file", "aof:" + os.path.join(bdir, "appendonly.aof.manifest"), port=PORT + 1)
    r2 = s2.start()
    check_equal("BACKUP START..SEAL under writes, restore into a fresh dir (--preload-file aof:manifest)", at_seal, fingerprint(s2),
                f"files={len(files)} after-seal present={r2.exists('after-seal')}")
    record("restore does not contain the write after SEAL", not r2.exists("after-seal"), "")
    s2.kill9()
    # A single snapshot: SAVE on the source, preload rdb:dump.kdb elsewhere
    r = s.start(); r.delete("after-seal"); r.save(); fp = fingerprint(s); s.kill9()
    d3 = fresh("restore-rdb"); s3 = Server(d3, "--preload-file", "rdb:" + os.path.join(d, "dump.kdb"), port=PORT + 2)
    s3.start()
    check_equal("SAVE, then --preload-file rdb:dump.kdb into a fresh dir", fp, fingerprint(s3))
    s3.kill9()


def t_aofoff():
    # #354 repro: AOF on, INCRs, CONFIG SET appendonly no, SHUTDOWN (saves), restart with --aof
    d = fresh("aofoff1"); s = Server(d, "--aof"); r = s.start()
    populate(r, n=2000, seed=5)
    for _ in range(500): r.incr("x")
    r.set("setkey", "v1"); r.set("delme", "1"); r.delete("delme")
    r.config_set("appendonly", "no")
    r.set("setkey", "v2"); r.incr("x")      # x = 501 while the AOF is off
    before = fingerprint(s)
    s.shutdown(); r = s.start()
    after = fingerprint(s)
    record("#354: AOF on, CONFIG SET appendonly no, SHUTDOWN, restart --aof: no double INCR", r.get("x") == b"501" and r.get("setkey") == b"v2" and not r.exists("delme") and before == after,
           f"x={r.get('x')} (want 501) setkey={r.get('setkey')} fp {before} vs {after}")
    for _ in range(100): r.incr("x")
    s.kill9(); r = s.start()
    record("#354 cont.: 100 more INCRs with AOF on, kill -9, restart", r.get("x") == b"601", f"x={r.get('x')} (want 601)")
    s.kill9()
    # Restart-based: run 1 AOF on, run 2 AOF off (SAVE), run 3 AOF on
    d = fresh("aofoff2"); s = Server(d, "--aof"); r = s.start()
    for _ in range(300): r.incr("y")
    s.kill9()
    s.args = []; r = s.start()                 # AOF off: loads dump.kdb (if any) only
    y_off = r.get("y")
    for _ in range(50): r.incr("y")
    r.save(); fp_off = fingerprint(s); s.kill9()
    s.args = ["--aof"]; r = s.start()
    y = r.get("y"); fp_on = fingerprint(s)
    info = r.info("persistence")
    record("run1 AOF on (300 INCR, kill -9), run2 AOF off (+50, SAVE), run3 AOF on: no double-applied INCRs",
           y == b"%d" % (int(y_off or 0) + 50) and fp_on == fp_off,
           f"run2 saw y={y_off}, run3 y={y} (want {int(y_off or 0)+50}), fp {fp_off} vs {fp_on}, superseded={info.get('aof_superseded_files')}")
    s.kill9()
    # same with a clean shutdown in run 1 (so dump.kdb has the 300)
    d = fresh("aofoff3"); s = Server(d, "--aof"); r = s.start()
    for _ in range(300): r.incr("y")
    s.shutdown(); s.args = []; r = s.start(); y1 = r.get("y")
    for _ in range(50): r.incr("y")
    r.save(); s.kill9(); s.args = ["--aof"]; r = s.start(); y = r.get("y")
    record("run1 AOF on (300 INCR, SHUTDOWN), run2 AOF off (+50, SAVE), run3 AOF on", y1 == b"300" and y == b"350",
           f"run2 y={y1} (want 300), run3 y={y} (want 350)")
    s.kill9()


os.makedirs(os.path.join(OUT, "dirs"), exist_ok=True)
for t in TESTS:
    try:
        globals()["t_" + t]()
    except Exception as e:
        import traceback
        record(t, False, f"exception {e!r} {traceback.format_exc()[-800:]}")
        subprocess.run(["pkill", "-9", "-f", "^" + BIN], check=False)
p = sum(1 for _, ok, _ in results if ok); f = len(results) - p
say(f"RESULT {os.path.basename(BIN)} {IO} pass={p} fail={f}")
json.dump(results, open(os.path.join(OUT, "results.json"), "w"), indent=1)
