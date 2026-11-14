#!/usr/bin/env python3
"""Compare a run's results with baseline.json; write summary.md and summary.json.

    compare.py <results-dir> [--baseline baseline.json] [--record] [--reset-gates]
               [--allow-vm-change]

<results-dir> holds results.jsonl (one line per engine x metric, written by
the suites) and env.json (written by run-all.sh).

Verdicts and exit status:
  ok         every gate passed                                        exit 0
  REGRESSED  a gate failed                                            exit 1
  INVALID    the run cannot be compared: a suite or server failed,    exit 1
             a recorded metric of a suite/config that ran is missing
             (or lacks an engine's value), nothing ran, the VM size
             differs from the recorded one, or the v1.2.2 or
             redis-server binary is not the pinned one
             (REGRESS_REDIS_SHA256=<sha> accepts a deliberate Redis change)
  SMOKE      SCALE != 1: gates are not checked, only that every       exit 0
             metric came out

--record writes this run's values into the baseline as the new recorded
values (v1.2.2, candidate and Redis, with the VM, date and candidate commit).
It refuses (exit 1, nothing written) when the run is INVALID, is a SCALE
smoke run, is not a three-engine run, or has a null value. A value is never
overwritten with null. A metric already in the baseline keeps its gate, so
tolerances tuned by hand survive a re-record; --reset-gates takes the gates
the suites declare instead. Metrics of suites/configs that did not run
(another --only, or no FULL=1) are left as they are and listed as "not run".
"""
import argparse
import datetime
import json
import os
import sys

ENG = ("v1_2_2", "candidate", "redis")


def load_results(path):
    by_key = {}
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            r = json.loads(line)
            key = "%s/%s/%s" % (r["suite"], r["config"], r["metric"])
            m = by_key.setdefault(key, {"suite": r["suite"], "config": r["config"], "metric": r["metric"],
                                        "unit": r["unit"], "better": r["better"], "gate": r["gate"],
                                        "values": {}, "samples": {}})
            m["values"][r["engine"]] = r["value"]
            if "samples" in r:
                m["samples"][r["engine"]] = r["samples"]
            if r["engine"] == "candidate":  # the candidate's declaration wins
                m["gate"], m["unit"], m["better"] = r["gate"], r["unit"], r["better"]
    return by_key


def fmt(v):
    if v is None:
        return "-"
    if isinstance(v, str):
        return v
    a = abs(v)
    if a >= 1000:
        return "{:,.0f}".format(v)
    if a >= 100:
        return "%.1f" % v
    if a >= 10:
        return "%.2f" % v
    return "%.3g" % v if a else "0"


def improvement(better, old, new):
    if old is None or new is None or old <= 0 or new <= 0:
        return None
    return old / new if better == "lower" else new / old


def check_gate(g, m, rec):
    """-> (status, limit_text). status: pass, FAIL, new, skip, info."""
    kind = g.get("kind")
    better = m["better"]
    now = m["values"]
    cand = now.get("candidate")
    if kind == "info":
        return "info", ""
    if cand is None:
        return "skip", "no candidate value"
    if kind == "rel":
        r = rec.get("candidate")
        if r is None:
            return "new", "rel %g%%: nothing recorded" % g["pct"]
        if better == "lower":
            lim = r * (1 + g["pct"] / 100.0)
            parts = ["%g%% over %s" % (g["pct"], fmt(r))]
            if "abs" in g:
                lim = max(lim, r + g["abs"])
                parts.append("+%g" % g["abs"])
            if "redis_plus" in g and now.get("redis") is not None:
                lim = max(lim, now["redis"] + g["redis_plus"])
                parts.append("Redis %s + %g" % (fmt(now["redis"]), g["redis_plus"]))
            return ("pass" if cand <= lim else "FAIL"), "<= %s (%s)" % (fmt(lim), ", ".join(parts))
        lim = r * (1 - g["pct"] / 100.0)
        if "abs" in g:
            lim = min(lim, r - g["abs"])
        return ("pass" if cand >= lim else "FAIL"), ">= %s (%g%% under %s)" % (fmt(lim), g["pct"], fmt(r))
    if kind == "ratio":
        imp_rec = improvement(better, rec.get("v1_2_2"), rec.get("candidate"))
        if imp_rec is None:
            return "new", "ratio: nothing recorded"
        imp_now = improvement(better, now.get("v1_2_2"), cand)
        if imp_now is None:
            return "skip", "ratio: v1.2.2 not measured this run"
        need = g["keep"] * imp_rec
        return ("pass" if imp_now >= need else "FAIL"), \
            "gain over v1.2.2 %.2fx >= %.2fx (%g%% of recorded %.2fx)" % (imp_now, need, g["keep"] * 100, imp_rec)
    if kind == "vs_redis":
        red = now.get("redis")
        if red is None or red <= 0:
            return "skip", "vs_redis: Redis not measured this run"
        x = cand / red if better == "lower" else red / cand if cand > 0 else float("inf")
        return ("pass" if x <= g["max_x"] else "FAIL"), "%.2fx Redis <= %gx" % (x, g["max_x"])
    if kind == "bound":
        texts, ok = [], True
        if "eq" in g:
            target = rec.get("candidate") if g["eq"] == "recorded" else g["eq"]
            if target is None:
                return "new", "eq: nothing recorded"
            ok &= cand == target
            texts.append("== %s" % fmt(target))
        if "max" in g:
            ok &= cand <= g["max"]
            texts.append("<= %s" % fmt(g["max"]))
        if "min" in g:
            ok &= cand >= g["min"]
            texts.append(">= %s" % fmt(g["min"]))
        return ("pass" if ok else "FAIL"), ", ".join(texts)
    return "FAIL", "unknown gate kind %r" % kind


def evaluate(results, baseline, smoke=False):
    rows = []
    for key in sorted(results):
        m = results[key]
        entry = baseline.get("metrics", {}).get(key)
        rec = (entry or {}).get("recorded", {})
        gates = (entry or {}).get("gate") or m["gate"]
        if isinstance(gates, dict):
            gates = [gates]
        if smoke:  # SCALE != 1: the numbers mean nothing
            checks = [(g, "smoke", "") for g in gates]
        else:
            checks = [(g,) + check_gate(g, m, rec) for g in gates]
        sts = [c[1] for c in checks]
        status = "FAIL" if "FAIL" in sts else "pass" if "pass" in sts else \
            "new" if "new" in sts else "skip" if "skip" in sts else "smoke" if "smoke" in sts else "info"
        rows.append({"key": key, "suite": m["suite"], "config": m["config"], "metric": m["metric"],
                     "unit": m["unit"], "better": m["better"], "values": m["values"],
                     "samples": m["samples"], "recorded_candidate": rec.get("candidate"),
                     "gates": [{"gate": g, "status": s, "limit": t} for g, s, t in checks],
                     "status": status})
    not_run = sorted(set(baseline.get("metrics", {})) - set(results))
    return rows, not_run


def validity_problems(env, baseline, results, smoke, allow_vm_change):
    """What makes this run's verdict meaningless; any one makes it INVALID."""
    probs = []
    if not env:
        probs.append("no env.json: run-all.sh did not finish")
    if env.get("run_failed"):
        probs.append("the run failed: %s" % "; ".join(env.get("failures") or ["see the log"]))
    if not results:
        probs.append("no results: nothing ran")
    metrics = baseline.get("metrics", {})
    # The machine size the values were recorded on.
    vms = {m.get("recorded", {}).get("vm") for m in metrics.values()} - {None}
    if vms and not smoke and env.get("vm") not in vms and not allow_vm_change:
        probs.append("VM size %r differs from the recorded %s" % (env.get("vm"), ", ".join(sorted(vms))))
    # The reference binaries the values were recorded with.
    pins = baseline.get("pins", {})
    engines = env.get("engines", {})
    selected = env.get("engines_selected") or list(ENG)
    want = pins.get("v1_2_2_sha256")
    got = engines.get("v1_2_2", {}).get("sha256")
    if want and "v1_2_2" in selected and got != want:
        probs.append("v1.2.2 binary sha256 %s is not the pinned %s" % (got, want))
    want = os.environ.get("REGRESS_REDIS_SHA256") or pins.get("redis_sha256")
    got = engines.get("redis", {}).get("sha256")
    if want and "redis" in selected and got != want:
        probs.append("redis-server sha256 %s is not the pinned %s (set REGRESS_REDIS_SHA256=<sha> for a "
                     "deliberate Redis change)" % (got, want))
    # Everything that should have come out did: every recorded metric of every
    # suite/config that ran, with a value from every engine that ran it.
    for exp in env.get("expected", []):
        engs = [e for e in exp["engines"] if e in selected]
        if not engs:
            continue
        prefix = "%s/%s/" % (exp["suite"], exp["config"])
        keys = {k for k in metrics if k.startswith(prefix)} | {k for k in results if k.startswith(prefix)}
        if not keys:
            probs.append("%s* produced no metrics" % prefix)
        for k in sorted(keys):
            if k not in results:
                probs.append("%s: missing" % k)
                continue
            for e in engs:
                if results[k]["values"].get(e) is None:
                    probs.append("%s: no %s value" % (k, e))
    return probs


def write_summary(outdir, rows, not_run, env, label, problems, smoke):
    fails = [r for r in rows if r["status"] == "FAIL"]
    verdict = "INVALID" if problems else "SMOKE" if smoke else "REGRESSED" if fails else "ok"
    summary = {"label": label, "env": env, "rows": rows, "not_run": not_run, "problems": problems,
               "failed": [r["key"] for r in fails], "verdict": verdict}
    with open(os.path.join(outdir, "summary.json"), "w") as f:
        json.dump(summary, f, separators=(",", ":"))  # one line: keeps result commits small
        f.write("\n")
    L = ["# Regression suite: %s" % label, ""]
    L.append("Verdict: **%s**%s" % (verdict, " (%d failed)" % len(fails) if fails else ""))
    L.append("")
    if problems:
        L += ["Why the run is invalid:", ""] + ["- %s" % p for p in problems] + [""]
    if smoke:
        L += ["SCALE=%s: a smoke run. The gates were not checked." % env.get("scale"), ""]
    if env:
        L.append("- Machine: %s, %s vCPU, %s, kernel %s, glibc %s" % (
            env.get("vm", "?"), env.get("nproc", "?"), env.get("cpu", "?"), env.get("kernel", "?"), env.get("glibc", "?")))
        L.append("- Client: Python %s, redis-py %s" % (env.get("python", "?"), env.get("redis_py", "?")))
        for e in ENG:
            d = env.get("engines", {}).get(e)
            if d:
                L.append("- %s: `%s`, sha256 `%s`" % (e, d.get("version", "?"), d.get("sha256", "?")))
        if env.get("candidate_commit"):
            L.append("- candidate commit: `%s`" % env["candidate_commit"])
        if env.get("date"):
            L.append("- date: %s; FULL=%s, SCALE=%s" % (env["date"], env.get("full", "?"), env.get("scale", "?")))
        if env.get("suite_seconds"):
            L.append("- run time: " + ", ".join("%s %ss" % kv for kv in sorted(env["suite_seconds"].items())))
        L.append("")
    suite = None
    for r in rows:
        if r["suite"] != suite:
            if suite is not None:
                L.append("")
            suite = r["suite"]
            L += ["## %s" % suite, "",
                  "| config | metric | unit | v1.2.2 | candidate | Redis | recorded | gate | result |",
                  "|---|---|---|---:|---:|---:|---:|---|---|"]
        v = r["values"]
        gate_txt = "<br>".join("%s: %s" % (g["gate"]["kind"], g["limit"]) if g["limit"] else g["gate"]["kind"]
                               for g in r["gates"])
        res = "<br>".join(g["status"] for g in r["gates"])
        L.append("| %s | %s | %s | %s | %s | %s | %s | %s | %s |" % (
            r["config"], r["metric"], r["unit"], fmt(v.get("v1_2_2")), fmt(v.get("candidate")),
            fmt(v.get("redis")), fmt(r["recorded_candidate"]), gate_txt, "**FAIL**" if r["status"] == "FAIL" else res))
    L.append("")
    if not_run:
        L += ["## Not run this time", "", ", ".join("`%s`" % k for k in not_run), ""]
    with open(os.path.join(outdir, "summary.md"), "w") as f:
        f.write("\n".join(L) + "\n")
    return summary


def record_problems(env, results, problems):
    probs = list(problems)
    if str(env.get("scale", "1")) not in ("1", "1.0"):
        probs.append("SCALE=%s: a smoke run is never recorded" % env.get("scale"))
    sel = env.get("engines_selected") or []
    if sorted(sel) != sorted(ENG):
        probs.append("not a three-engine run (engines: %s)" % ",".join(sel))
    for k, m in results.items():
        for e in ENG:
            if e in m["values"] and m["values"][e] is None:
                probs.append("%s: %s value is null" % (k, e))
    return probs


def record(baseline, results, env, label, reset_gates):
    ms = baseline.setdefault("metrics", {})
    date = env.get("date") or datetime.datetime.utcnow().strftime("%Y-%m-%d")
    for key, m in sorted(results.items()):
        old = ms.get(key, {})
        old_rec = old.get("recorded", {})
        rec = {}
        for e in ENG:  # never overwrite a value with null
            v = m["values"].get(e)
            rec[e] = v if v is not None else old_rec.get(e)
        rec.update({"vm": env.get("vm"), "date": date, "commit": env.get("candidate_commit"), "label": label})
        ms[key] = {
            "suite": m["suite"], "config": m["config"], "metric": m["metric"],
            "unit": m["unit"], "better": m["better"], "recorded": rec,
            "gate": m["gate"] if (reset_gates or "gate" not in old) else old["gate"],
        }
    baseline["metrics"] = dict(sorted(ms.items()))
    pins = baseline.setdefault("pins", {})
    eng = env.get("engines", {})
    if eng.get("v1_2_2", {}).get("sha256"):
        pins.setdefault("v1_2_2_sha256", eng["v1_2_2"]["sha256"])
    if eng.get("redis", {}).get("sha256") and (os.environ.get("REGRESS_REDIS_SHA256") or "redis_sha256" not in pins):
        pins["redis_sha256"] = eng["redis"]["sha256"]
    return baseline


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("results_dir")
    ap.add_argument("--baseline", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "baseline.json"))
    ap.add_argument("--record", action="store_true")
    ap.add_argument("--reset-gates", action="store_true")
    ap.add_argument("--allow-vm-change", action="store_true",
                    help="compare or record on another VM size than the recorded one")
    ap.add_argument("--label")
    a = ap.parse_args()
    rpath = os.path.join(a.results_dir, "results.jsonl")
    results = load_results(rpath) if os.path.exists(rpath) else {}
    env = {}
    if os.path.exists(os.path.join(a.results_dir, "env.json")):
        with open(os.path.join(a.results_dir, "env.json")) as f:
            env = json.load(f)
    baseline = {"metrics": {}}
    if os.path.exists(a.baseline):
        with open(a.baseline) as f:
            baseline = json.load(f)
    smoke = str(env.get("scale", "1")) not in ("1", "1.0")
    label = a.label or env.get("label") or os.path.basename(os.path.abspath(a.results_dir))
    problems = validity_problems(env, baseline, results, smoke, a.allow_vm_change)
    rows, not_run = evaluate(results, baseline, smoke)
    summary = write_summary(a.results_dir, rows, not_run, env, label, problems, smoke)
    for p in problems:
        print("INVALID: %s" % p)
    for r in rows:
        if r["status"] == "FAIL":
            print("FAIL %s: candidate %s; %s" % (r["key"], fmt(r["values"].get("candidate")),
                                                 "; ".join(g["limit"] for g in r["gates"] if g["status"] == "FAIL")))
    print("verdict: %s (%d metrics, %d failed, %d not run)" % (
        summary["verdict"], len(rows), len(summary["failed"]), len(not_run)))
    if a.record:
        refuse = record_problems(env, results, problems)
        if refuse:
            for p in refuse:
                print("not recording: %s" % p)
            return 1
        if summary["failed"]:
            print("recording over %d failed gates: commit only if each change is explained" % len(summary["failed"]))
        record(baseline, results, env, label, a.reset_gates)
        with open(a.baseline, "w") as f:
            json.dump(baseline, f, indent=2)
            f.write("\n")
        print("recorded %d metrics into %s" % (len(results), a.baseline))
        return 0
    return 1 if (problems or summary["failed"]) else 0


if __name__ == "__main__":
    sys.exit(main())
