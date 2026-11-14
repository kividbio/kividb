"""What every suite under suites/ is built on.

A suite is one Python file that declares its configurations (each one is a
fresh server: which engines, which I/O model, how many threads) and a run
function that measures one engine in one configuration and emits metrics.
run-all.sh starts and stops the servers; a suite only talks to the port it is
given. See README.md, "Adding a suite".

    from suite import Suite, Config, rel, ratio, vs_redis, bound, info

    S = Suite("05-example", [
        Config("tokio", io="tokio"),
        Config("uring1", io="uring", threads=1, engines=("v1_2_2", "candidate")),
        Config("big", full_only=True),          # only with FULL=1
    ])

    @S.run
    def run(ctx):
        c = ctx.client()
        ...
        ctx.emit("load_s", 12.3, "s", "lower", gate=rel(10))
        ctx.raw["spikes"] = [...]               # kept in raw.tar.gz

    if __name__ == "__main__":
        S.main()

Gates (one, or a list, per metric; compare.py explains each in summary.md):
    rel(pct, abs=None, redis_plus=None)
        candidate within pct % of its recorded value, in the worse direction.
        abs: also allow this much in the metric's unit (the larger slack wins).
        redis_plus: also allow up to Redis's value this session + this much.
    ratio(keep)
        the improvement over v1.2.2 measured this session (v1.2.2/candidate for
        lower-is-better, candidate/v1.2.2 for higher-is-better) is at least
        keep x the recorded improvement.
    vs_redis(x)
        candidate is at most x times worse than Redis this session.
    bound(eq=, max=, min=)
        absolute; eq="recorded" means equal to the recorded candidate value.
    info()
        recorded and shown, never gated.
"""
import argparse
import json
import os
import sys
import time
import traceback

ENGINES = ("v1_2_2", "candidate", "redis")


def rel(pct, abs=None, redis_plus=None):
    g = {"kind": "rel", "pct": pct}
    if abs is not None:
        g["abs"] = abs
    if redis_plus is not None:
        g["redis_plus"] = redis_plus
    return g


def ratio(keep=0.6):
    return {"kind": "ratio", "keep": keep}


def vs_redis(x):
    return {"kind": "vs_redis", "max_x": x}


def bound(eq=None, max=None, min=None):
    g = {"kind": "bound"}
    if eq is not None:
        g["eq"] = eq
    if max is not None:
        g["max"] = max
    if min is not None:
        g["min"] = min
    return g


def info():
    return {"kind": "info"}


# The plan's standard tolerances, so suites say which one they mean.
THROUGHPUT = rel(10)                            # time or rate: 10 % worse
MEMORY = rel(5)                                 # memory held: 5 % more
GIVE_BACK = rel(25, abs=5)                      # RSS after a give-back: 25 % or 5 MiB
WORST_PING = rel(50, redis_plus=10)             # max(1.5x recorded, Redis + 10 ms)
KEEP_IMPROVEMENT = ratio(0.6)                   # keep 60 % of the gain over v1.2.2
EXACT = bound(eq="recorded")                    # counts


class Config:
    """One configuration: a fresh server per engine, started by run-all.sh.

    self_hosted=True: run-all.sh starts no server; the suite starts its own
    from ctx.bin (several nodes, a cluster: lib/nodes.py), and ctx.port and
    ctx.pid are None."""

    def __init__(self, name, io="auto", threads=0, engines=ENGINES, full_only=False, self_hosted=False):
        self.name, self.io, self.threads = name, io, threads
        self.engines, self.full_only = tuple(engines), full_only
        self.self_hosted = self_hosted

    def as_dict(self):
        return {"name": self.name, "io": self.io, "threads": self.threads,
                "engines": list(self.engines), "full_only": self.full_only,
                "self_hosted": self.self_hosted}


class Ctx:
    def __init__(self, suite, config, engine, kind, port, pid, out, bin=None):
        self.suite, self.config, self.engine, self.kind = suite, config, engine, kind
        self.port, self.pid, self.out, self.bin = port, pid, out, bin
        self.raw = {}
        self.metrics = []
        self.t_start = time.time()

    def client(self, **kw):
        import redis
        kw.setdefault("socket_timeout", 600)
        kw.setdefault("port", self.port)
        return redis.Redis(host="127.0.0.1", **kw)

    def log(self, msg):
        print("[%s %s %s] %s" % (self.suite, self.config.name, self.engine, msg), flush=True)

    def emit(self, metric, value, unit, better, gate=None, samples=None, note=None):
        assert better in ("lower", "higher"), better
        gates = gate if isinstance(gate, list) else ([gate] if gate else [info()])
        rec = {"suite": self.suite, "config": self.config.name, "engine": self.engine,
               "metric": metric, "unit": unit, "better": better,
               "value": None if value is None else round(float(value), 4), "gate": gates}
        if samples is not None:
            rec["samples"] = [round(float(s), 4) for s in samples]
        if note:
            rec["note"] = note
        self.metrics.append(rec)
        self.log("%s = %s %s%s" % (metric, rec["value"], unit,
                                   "  (%s)" % " / ".join("%g" % s for s in rec.get("samples", [])) if samples else ""))

    def server_info(self, port=None):
        port = port or self.port
        if port is None:
            return {}
        try:
            i = self.client(port=port).info("server")
            keep = ("redis_version", "kividb_version", "multiplexing_api", "io_threads_active",
                    "process_id", "arch_bits", "gcc_version")
            return {k: i[k] for k in i if k in keep or "version" in k or "multiplexing" in k}
        except Exception as e:  # informational only
            return {"error": str(e)}


class Suite:
    def __init__(self, name, configs):
        self.name, self.configs, self.fn = name, configs, None

    def run(self, fn):
        self.fn = fn
        return fn

    def active_configs(self):
        full = os.environ.get("FULL", "0") not in ("", "0")
        return [c for c in self.configs if full or not c.full_only]

    def main(self, argv=None):
        ap = argparse.ArgumentParser(description="regression suite " + self.name)
        ap.add_argument("--configs", action="store_true", help="print the configurations as JSON lines")
        ap.add_argument("--config")
        ap.add_argument("--engine", choices=ENGINES)
        ap.add_argument("--kind", choices=("kividb", "redis"))
        ap.add_argument("--port", type=int)
        ap.add_argument("--pid", type=int)
        ap.add_argument("--bin", help="the engine's server binary (for suites that start their own)")
        ap.add_argument("--out", help="results directory (results.jsonl and raw/)")
        a = ap.parse_args(argv)
        if a.configs:
            for c in self.active_configs():
                print(json.dumps(c.as_dict()))
            return 0
        cfg = next((c for c in self.configs if c.name == a.config), None)
        if cfg is None:
            ap.error("unknown config %r" % a.config)
        ctx = Ctx(self.name, cfg, a.engine, a.kind, a.port, a.pid, a.out, a.bin)
        ctx.raw["server"] = ctx.server_info()
        status = 0
        try:
            self.fn(ctx)
        except Exception:
            traceback.print_exc()
            ctx.raw["error"] = traceback.format_exc()
            status = 1
        ctx.raw["seconds"] = round(time.time() - ctx.t_start, 1)
        os.makedirs(os.path.join(a.out, "raw", self.name), exist_ok=True)
        with open(os.path.join(a.out, "results.jsonl"), "a") as f:
            for m in ctx.metrics:
                f.write(json.dumps(m) + "\n")
        with open(os.path.join(a.out, "raw", self.name, "%s-%s.json" % (cfg.name, a.engine)), "w") as f:
            json.dump({"metrics": ctx.metrics, **ctx.raw}, f, indent=1)
        return status


def scaled(n, step=2000):
    """n times $SCALE (default 1), a multiple of step, at least step.

    SCALE=0.01 makes a quick smoke run of a suite's code paths. Its numbers mean
    nothing and its counts will not match the baseline: never record one."""
    f = float(os.environ.get("SCALE", "1") or 1)
    if f == 1:
        return n
    return max(step, int(n * f) // step * step)


def median(xs):
    xs = sorted(x for x in xs if x is not None)
    if not xs:
        return None
    n = len(xs)
    return xs[n // 2] if n % 2 else (xs[n // 2 - 1] + xs[n // 2]) / 2


if __name__ == "__main__":
    sys.exit("import this module from a suite")
