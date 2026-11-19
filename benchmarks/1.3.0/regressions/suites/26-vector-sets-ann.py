#!/usr/bin/env python3
"""#627: vector sets on real embeddings, against Redis 8.10.2 and Garnet, at
matched recall.

Suite 09 measures quantized storage (#697) on synthetic clusters; this one
measures the search itself on real data, so the two keep one place per
number: 09 for what Q8/BIN cost and keep, 26 for VADD/VSIM against the other
servers.

Data (downloaded once into $VSET_DATASETS, default $REGRESS_CACHE/datasets,
SHA-256 checked):

  glove100  glove-100-angular (ann-benchmarks), 100 dimensions, cosine
  laion768  laion-img-emb-768-1M-cosine (redis/vector-db-benchmark), 768
            dimensions, cosine

Configurations: the first 100,000 vectors of each (`glove100_100k`,
`laion768_100k`), and the first 1,000,000 with VSET_1M=1 (`glove100_1m`,
`laion768_1m`; not with FULL=1: Redis loads them at under 1,000 VADD/s, and
Garnet runs in the same window, so they need SUITE_TIMEOUT=7200). Queries: the first 500 of
the dataset's test vectors. Ground truth: the exact top 10 by cosine over the
vectors loaded (not the dataset's own, which is over all of them), computed
with numpy and cached beside the dataset.

Every server gets the same commands:

  VADD vs FP32 <blob> <i> Q8 [XDISTANCE_METRIC COSINE]   (pipelined 500 at a
       time from 4 client processes; the metric option is Garnet's, which
       defaults to L2; Redis and KiviDB are cosine only)
  VSIM vs FP32 <blob> COUNT 10 EF <ef>

Q8 is Redis's and Garnet's default quantization; KiviDB's default is f32, so
it is named on every VADD. VINFO is kept in `raw` for each server.

Matched recall, not matched parameters: each server's EF is the first of
10, 20, 40, ..., 1280 at which recall@10 over the 500 queries (one client) is
at least 0.95 (`TARGET`), and every q/s and p99 row is measured at that EF.
KiviDB's VSIM is an exact scan today (#710), over the Q8 vectors: its recall
is what Q8 leaves (0.98 on glove100, 0.976 on laion768) at any EF, so it
stops at the first. A server that never gets there is measured at 1280 and
says so in the row's note.

Rows, per configuration:

  vadd_per_s                 vectors loaded a second
  bytes_per_vector           INFO used_memory after the load, less before, a
                             vector (KiviDB, Redis)
  rss_per_vector             the server's RSS after the load, less before, a
                             vector (all three, Garnet has no used_memory)
  ef, recall_at_10           the EF chosen and the recall it gave
  vsim_qps_c1/_c16/_c100     VSIM a second at 1, 16 and 100 connections (4
                             client processes, asyncio; 2 s warm-up, 10 s)
  vsim_p99_ms_c1/_c16/_c100  their p99

Garnet: in the `redis` engine's run the suite starts $GARNET_SERVER, the
GarnetServer of Garnet v2.2.13 (`linux-x64-based.tar.xz`, SHA-256
23a4f19d5ffd1c533f1db7cc9ef4c00a5f811aa2914544cc819fd23215fa557c, on .NET
10), with `--enable-vector-set-preview` (GARNET_ARGS adds to `--index 1g
--memory 64g`, the 1.2.0 suite's flags), and measures it the same way. Its
numbers go into `raw["garnet"]`, not into rows: compare.py wants every row
from every engine. Garnet's vector sets are a preview (DiskANN, `EF` is its
L_search). GARNET_SERVER=none leaves Garnet out, for a run on a machine
without it; anything else that is not an executable fails the suite before
it loads anything.

Gates (candidate): against Redis only where KiviDB is ahead, as #627 asks:
`vadd_per_s` at least 10x Redis's and `bytes_per_vector` at most 0.8x (no
graph to build or hold; measured off the VM at 120-180x and 0.23-0.57x).
recall_at_10 at least TARGET. The q/s rows hold against the
recorded candidate within 25 % (suite 09's tolerance for VSIM q/s); KiviDB
loses them to Redis's HNSW search (4-40x at 100,000 vectors, more at 1M:
#710), which the rows record. p99 is shown.

Needs numpy and h5py (requirements.txt).
"""
import asyncio
import hashlib
import json
import multiprocessing as mp
import os
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.request

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib"))
from suite import Suite, Config, THROUGHPUT, MEMORY, bound, info, jitter, rel, vs_redis, scaled  # noqa: E402
from ports import free_port  # noqa: E402

DATASETS = {
    "glove100": {
        "file": "glove-100-angular.hdf5",
        "url": "https://ann-benchmarks.com/glove-100-angular.hdf5",
        "sha256": "544af1d5e84e112cd4749571dcfd8ca109818a572f850af75a3a09e093a953c4",
    },
    "laion768": {
        "file": "laion-img-emb-768-1M-cosine.hdf5",
        # Path-style: the bucket's name has a dot, which the virtual-host
        # name's certificate does not cover.
        "url": "https://s3.amazonaws.com/benchmarks.redislabs/vecsim/laion400m/laion-img-emb-768-1M-cosine.hdf5",
        "sha256": "86376b43c51bbdd484b6ab58365e34d9ee7568d0b94af98ee37535e1585ba753",
    },
}
# name: (dataset, vectors, VSET_1M=1 only)
CONFIGS = {
    "glove100_100k": ("glove100", 100_000, False),
    "laion768_100k": ("laion768", 100_000, False),
    "glove100_1m": ("glove100", 1_000_000, True),
    "laion768_1m": ("laion768", 1_000_000, True),
}
K = 10
TARGET = 0.95
EFS = (10, 20, 40, 80, 160, 320, 640, 1280)
NQ = 500
QUANT = "Q8"
KEY = "vs"
LOAD_CLIENTS = 4
BATCH = 500
CLIENTS = (1, 16, 100)
PROCS = int(os.environ.get("VSET_PROCS", "4"))
SECONDS = 10.0
WARMUP = 2.0
# Where KiviDB is ahead of Redis (vs_redis: Redis / candidate for a rate,
# candidate / Redis for memory, at most this).
VADD_VS_REDIS = 0.1
MEMORY_VS_REDIS = 0.8

BIG = os.environ.get("VSET_1M", "0") not in ("", "0")
S = Suite("26-vector-sets-ann", [Config(n) for n, (_, _, big) in CONFIGS.items() if BIG or not big])

# Set before the client processes fork, so they share them.
DATA = QUERIES = None
TARGET_PORT = None
VADD_EXTRA = ()


# ── data ─────────────────────────────────────────────────────────────────────

def dataset_dir():
    d = os.environ.get("VSET_DATASETS") or os.path.join(
        os.environ.get("REGRESS_CACHE") or os.path.expanduser("~/.cache/kividb-regress"), "datasets")
    os.makedirs(d, exist_ok=True)
    return d


def sha256_of(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def fetch(name):
    """The dataset's file, downloaded once and checked against its pinned SHA-256."""
    ds = DATASETS[name]
    path = os.path.join(dataset_dir(), ds["file"])
    if not os.path.exists(path):
        part = path + ".part"
        # ann-benchmarks.com answers urllib's own User-Agent with 403.
        req = urllib.request.Request(ds["url"], headers={"User-Agent": "kividb-regress"})
        with urllib.request.urlopen(req, timeout=60) as r, open(part, "wb") as f:
            shutil.copyfileobj(r, f, 1 << 20)
        os.replace(part, path)
    got = sha256_of(path)
    if got != ds["sha256"]:
        raise RuntimeError("%s: SHA-256 %s, want %s (delete it to fetch it again)" % (path, got, ds["sha256"]))
    return path


def load_data(name, n):
    """(vectors, queries, exact top-K ids of each query among the vectors)."""
    import h5py
    import numpy as np
    path = fetch(name)
    with h5py.File(path, "r") as f:
        if f["train"].shape[0] < n:
            raise RuntimeError("%s holds %d vectors, not %d" % (path, f["train"].shape[0], n))
        data = np.ascontiguousarray(f["train"][:n], dtype="<f4")
        queries = np.ascontiguousarray(f["test"][:NQ], dtype="<f4")
    cache = "%s.n%d.q%d.top%d.npy" % (path, n, NQ, K)
    if os.path.exists(cache):
        return data, queries, np.load(cache)
    unit = data / np.maximum(np.linalg.norm(data, axis=1, keepdims=True), 1e-12)
    qunit = queries / np.maximum(np.linalg.norm(queries, axis=1, keepdims=True), 1e-12)
    truth = np.empty((len(queries), K), dtype=np.int64)
    for s in range(0, len(queries), 50):
        sims = qunit[s:s + 50] @ unit.T
        part = np.argpartition(-sims, K, axis=1)[:, :K]
        for j in range(len(part)):
            truth[s + j] = part[j][np.argsort(-sims[j, part[j]])]
    np.save(cache, truth)
    return data, queries, truth


# ── one server ───────────────────────────────────────────────────────────────

def rss_bytes(pid):
    try:
        with open("/proc/%d/status" % pid) as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) * 1024
    except OSError:
        pass
    out = subprocess.run(["ps", "-o", "rss=", "-p", str(pid)], capture_output=True, text=True)
    return int(out.stdout.strip()) * 1024


def used_memory(c):
    m = c.info("memory")
    return int(m["used_memory"]) if "used_memory" in m else None


def load_worker(rank):
    import redis
    c = redis.Redis(port=TARGET_PORT, socket_timeout=600)
    n = len(DATA)
    lo, hi = n * rank // LOAD_CLIENTS, n * (rank + 1) // LOAD_CLIENTS
    for s in range(lo, hi, BATCH):
        p = c.pipeline(transaction=False)
        for i in range(s, min(hi, s + BATCH)):
            p.execute_command("VADD", KEY, "FP32", DATA[i].tobytes(), str(i), QUANT, *VADD_EXTRA)
        for res in p.execute(raise_on_error=False):
            if isinstance(res, Exception):
                return "VADD: %s" % res
    c.close()
    return None


def vsim(qi, ef):
    return ["VSIM", KEY, "FP32", QUERIES[qi].tobytes(), "COUNT", str(K), "EF", str(ef)]


def recall_at(c, ef, truth):
    hits = 0
    for qi in range(len(QUERIES)):
        got = {int(x) for x in c.execute_command(*vsim(qi, ef))}
        hits += len(got & set(truth[qi].tolist()))
    return hits / (K * len(QUERIES))


async def query_conn(conn, nconns, ef, deadline, lat):
    import redis.asyncio as aredis
    r = aredis.Redis(port=TARGET_PORT, socket_timeout=600)
    qi = conn % len(QUERIES)
    try:
        while time.perf_counter() < deadline:
            t = time.perf_counter()
            await r.execute_command(*vsim(qi, ef))
            lat.append(time.perf_counter() - t)
            qi = (qi + nconns) % len(QUERIES)
    finally:
        await r.aclose()


def query_worker(job):
    rank, procs, nconns, ef, deadline = job
    lat = []

    async def go():
        await asyncio.gather(*(query_conn(c, nconns, ef, deadline, lat)
                               for c in range(rank, nconns, procs)))

    t0 = time.perf_counter()
    asyncio.run(go())
    return lat, t0, time.perf_counter()


def query_cell(nconns, ef):
    """(VSIM a second, p99 ms) at nconns connections."""
    import numpy as np
    procs = min(nconns, PROCS)
    with mp.get_context("fork").Pool(procs) as pool:
        deadline = time.perf_counter() + WARMUP
        pool.map(query_worker, [(p, procs, nconns, ef, deadline) for p in range(procs)])
        deadline = time.perf_counter() + SECONDS
        parts = pool.map(query_worker, [(p, procs, nconns, ef, deadline) for p in range(procs)])
    secs = max(p[2] for p in parts) - min(p[1] for p in parts)
    lat = np.array([x for p in parts for x in p[0]]) * 1000.0
    if not len(lat):
        raise RuntimeError("no VSIM answered at %d connections" % nconns)
    return len(lat) / secs, float(np.percentile(lat, 99))


def measure(port, pid, truth, extra=()):
    """Load, then search at matched recall, one server -> rows and raw."""
    import redis
    global TARGET_PORT, VADD_EXTRA
    TARGET_PORT, VADD_EXTRA = port, tuple(extra)
    c = redis.Redis(port=port, socket_timeout=600)
    c.delete(KEY)
    # A server just started (Garnet's .NET runtime) gives some of its start-up
    # memory back in its first seconds.
    time.sleep(3)
    mem0, rss0 = used_memory(c), rss_bytes(pid)
    phase = {}
    t0 = time.perf_counter()
    with mp.get_context("fork").Pool(LOAD_CLIENTS) as pool:
        errors = [e for e in pool.map(load_worker, range(LOAD_CLIENTS)) if e]
    load_s = time.perf_counter() - t0
    if errors:
        raise RuntimeError(errors[0])
    card = int(c.execute_command("VCARD", KEY))
    if card != len(DATA):
        raise RuntimeError("VCARD %d after loading %d" % (card, len(DATA)))
    time.sleep(2)
    mem1, rss1 = used_memory(c), rss_bytes(pid)
    vinfo = c.execute_command("VINFO", KEY)
    if isinstance(vinfo, list):
        vinfo = dict(zip(vinfo[0::2], vinfo[1::2]))
    dec = lambda v: v.decode() if isinstance(v, bytes) else v  # noqa: E731
    raw = {"vinfo": {dec(k): dec(v) for k, v in vinfo.items()}, "load_s": round(load_s, 2),
           "vadd_extra": list(extra), "ladder": [], "phase_s": phase}
    phase["load"] = round(load_s, 1)
    t = time.perf_counter()

    ef, recall = None, 0.0
    for ef in EFS:
        recall = recall_at(c, ef, truth)
        raw["ladder"].append([ef, round(recall, 4)])
        if recall >= TARGET:
            break
    phase["ladder"] = round(time.perf_counter() - t, 1)
    rows = {
        "vadd_per_s": len(DATA) / load_s,
        "bytes_per_vector": None if mem0 is None else (mem1 - mem0) / len(DATA),
        "rss_per_vector": (rss1 - rss0) / len(DATA),
        "ef": ef,
        "recall_at_10": recall,
    }
    t = time.perf_counter()
    for n in CLIENTS:
        qps, p99 = query_cell(n, ef)
        rows["vsim_qps_c%d" % n], rows["vsim_p99_ms_c%d" % n] = qps, p99
    phase["queries"] = round(time.perf_counter() - t, 1)
    t = time.perf_counter()
    c.delete(KEY)
    phase["delete"] = round(time.perf_counter() - t, 1)
    return rows, raw


# ── Garnet ───────────────────────────────────────────────────────────────────

def garnet_binary():
    """$GARNET_SERVER, or None for GARNET_SERVER=none; anything else fails."""
    binary = os.environ.get("GARNET_SERVER", "")
    if binary == "none":
        return None
    if not binary or not os.access(binary, os.X_OK):
        raise RuntimeError("$GARNET_SERVER (GarnetServer, or none) is not set to an executable")
    return binary


def garnet(binary, truth):
    """Garnet measured beside Redis -> its rows and raw, for raw["garnet"]."""
    import redis
    port = free_port()
    workdir = tempfile.mkdtemp(prefix="kividb-regress-garnet.")
    cmd = [binary, "--port", str(port), "--bind", "127.0.0.1", "--enable-vector-set-preview",
           "--index", "1g", "--memory", "64g"] + os.environ.get("GARNET_ARGS", "").split()
    log = open(os.path.join(workdir, "garnet.log"), "wb")
    proc = subprocess.Popen(cmd, cwd=workdir, stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT)
    pidfile = None
    if os.environ.get("REGRESS_PIDDIR"):
        pidfile = os.path.join(os.environ["REGRESS_PIDDIR"], str(proc.pid))
        with open(pidfile, "w") as f:
            f.write(binary)
    try:
        c = redis.Redis(port=port, socket_timeout=5)
        for _ in range(300):
            try:
                if c.ping():
                    break
            except redis.ConnectionError:
                if proc.poll() is not None:
                    raise RuntimeError("Garnet exited %s" % proc.returncode)
                time.sleep(0.1)
        else:
            raise RuntimeError("Garnet did not answer PING")
        server = c.info("server")
        rows, raw = measure(port, proc.pid, truth, extra=("XDISTANCE_METRIC", "COSINE"))
        raw["server"] = {k: v for k, v in server.items() if "version" in k}
        raw["cmd"] = cmd
        raw["rows"] = rows
        return raw
    finally:
        proc.terminate()
        try:
            proc.wait(10)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
        log.close()
        if pidfile:
            os.remove(pidfile)
        shutil.rmtree(workdir, ignore_errors=True)


# ── rows ─────────────────────────────────────────────────────────────────────

def emit(ctx, rows):
    note = None if rows["recall_at_10"] >= TARGET else "recall %.3f under %.2f at EF %d" % (
        rows["recall_at_10"], TARGET, EFS[-1])
    ctx.emit("vadd_per_s", rows["vadd_per_s"], "VADD/s", "higher",
             gate=[THROUGHPUT, vs_redis(VADD_VS_REDIS)])
    ctx.emit("bytes_per_vector", rows["bytes_per_vector"], "B", "lower",
             gate=[MEMORY, vs_redis(MEMORY_VS_REDIS)])
    ctx.emit("rss_per_vector", rows["rss_per_vector"], "B", "lower", gate=info())
    ctx.emit("ef", rows["ef"], "EF", "lower", gate=info(), note=note)
    ctx.emit("recall_at_10", rows["recall_at_10"], "ratio", "higher", gate=bound(min=TARGET), note=note)
    for n in CLIENTS:
        ctx.emit("vsim_qps_c%d" % n, rows["vsim_qps_c%d" % n], "q/s", "higher", gate=rel(25), note=note)
        ctx.emit("vsim_p99_ms_c%d" % n, rows["vsim_p99_ms_c%d" % n], "ms", "lower",
                 gate=jitter(info()), note=note)


@S.run
def run(ctx):
    global DATA, QUERIES
    # Before anything loads: a run that cannot start Garnet fails at once.
    garnet_bin = garnet_binary()
    name, n, _ = CONFIGS[ctx.config.name]
    n = scaled(n, step=2000)
    DATA, QUERIES, truth = load_data(name, n)
    ctx.raw["data"] = {"dataset": name, "vectors": n, "dim": int(DATA.shape[1]), "queries": NQ,
                       "target_recall": TARGET, "quant": QUANT, "procs": PROCS}
    ctx.raw["clients_cpus"] = os.cpu_count()
    rows, raw = measure(ctx.port, ctx.pid, truth)
    ctx.raw.update(raw)
    emit(ctx, rows)
    if ctx.engine == "redis":
        ctx.raw["garnet"] = garnet(garnet_bin, truth) if garnet_bin else "GARNET_SERVER=none"


if __name__ == "__main__":
    sys.exit(S.main())
