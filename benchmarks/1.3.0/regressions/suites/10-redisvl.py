#!/usr/bin/env python3
"""#767: FT.HYBRID takes SCORER and replies as Redis 8.10 does (#779).

Runs the RedisVL 0.32.0 harness, benchmarks/redisvl-compat/ (67 checks: hash
and JSON indexes, HybridQuery, AggregateHybridQuery, SemanticCache,
SemanticRouter), unchanged, against each server. It runs in its own venv
($REGRESS_CACHE/venv-redisvl) with that folder's pinned requirements.txt
(redisvl 0.32.0, redis-py 8.1.0), which differ from this suite's client.

KiviDB only: the Redis 8.10.2 on the VM image has no Query Engine or JSON
module. #779 recorded Redis 8.10.3 (the redis:8.10 image) at 67 of 67.

Gate: the checks passed equal the recorded count (67), and none fail.
"""
import json
import os
import shutil
import subprocess
import sys
import tempfile

here = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(here, "..", "lib"))
from suite import Suite, Config, EXACT, bound, info  # noqa: E402

HARNESS = os.path.normpath(os.path.join(here, "..", "..", "..", "redisvl-compat"))
S = Suite("10-redisvl", [Config("redisvl", engines=("v1_2_2", "candidate"))])


def venv():
    cache = os.environ.get("REGRESS_CACHE", os.path.expanduser("~/.cache/kividb-regress"))
    v = os.path.join(cache, "venv-redisvl")
    req = os.path.join(HARNESS, "requirements.txt")
    stamp = os.path.join(v, ".requirements")
    py = os.path.join(v, "bin", "python")
    same = os.path.exists(stamp) and open(stamp).read() == open(req).read()
    if not (os.access(py, os.X_OK) and same):
        shutil.rmtree(v, ignore_errors=True)
        subprocess.run(["python3", "-m", "venv", v], check=True)
        subprocess.run([os.path.join(v, "bin", "pip"), "install", "-q", "--disable-pip-version-check",
                        "-r", req], check=True)
        shutil.copy(req, stamp)
    return py


@S.run
def run(ctx):
    py = venv()
    ctx.client().flushall()
    work = tempfile.mkdtemp(prefix="kividb-regress-redisvl.")
    try:
        # The harness writes results-<label>.json next to itself: run a copy.
        script = os.path.join(work, "redisvl_kividb_compat.py")
        shutil.copy(os.path.join(HARNESS, "redisvl_kividb_compat.py"), script)
        p = subprocess.run([py, "-I", script, "--url", "redis://127.0.0.1:%d" % ctx.port, "--label", ctx.engine],
                           cwd=work, capture_output=True, text=True, timeout=900)
        ctx.raw["stdout_tail"] = p.stdout[-8000:]
        with open(os.path.join(work, "results-%s.json" % ctx.engine)) as f:
            res = json.load(f)
    finally:
        shutil.rmtree(work, ignore_errors=True)
    ctx.raw["results"] = res
    ctx.emit("checks_passed", res["counts"]["PASS"], "checks", "higher", gate=EXACT)
    ctx.emit("checks_failed", res["counts"]["FAIL"], "checks", "lower", gate=bound(max=0))
    failed = sorted({r.get("check", r.get("name", "?")) for r in res["checks"] if r["status"] != "PASS"})
    ctx.emit("lua_commands_sent", len(res["lua_commands"]), "commands", "lower", gate=info(),
             note=("failed: " + "; ".join(failed)) if failed else None)


if __name__ == "__main__":
    sys.exit(S.main())
