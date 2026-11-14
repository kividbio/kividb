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

Host speed. The same VM size can land on different CPU generations (two
Standard_D4s_v5 differed by ~40 %), so a `rel` gate on a speed-dependent metric
(unit ms, s, us or */s; a gate's "speed": false/true overrides) is not judged
against the recorded absolute. v1.2.2 is measured in every session, so the
recorded candidate value is first scaled to this host:
  - the host factor of the metric's suite/config: the median, over that
    suite/config's speed metrics that have a v1.2.2 value both recorded and now
    (at least 3), of "this host is X times as fast as the recorded one" (rates
    now/recorded, times recorded/now);
  - otherwise the same median over all suites (global factor);
  - a gate with "anchor": true uses its own metric's v1.2.2 ratio instead. Not
    the default: on the committed runs (same host) single-metric v1.2.2 ratios
    strayed 15-45 % while the medians stayed within 2 %.
Bytes, counts, recall and ratios are never scaled. `bound` gates stay absolute;
a global factor under 0.7 or over 1.4 only adds a WARN line to the summary.

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

# The host-speed rules: see the module docstring.
SPEED_UNITS = {"ms", "s", "us", "ns"}
MIN_GROUP = 3  # speed metrics a suite/config needs for its own factor
FACTOR_WARN_LOW, FACTOR_WARN_HIGH = 0.7, 1.4


def is_speed_metric(unit, gate=None):
    """Does the metric's value move with the host's speed (times, rates)?"""
    if gate is not None and "speed" in gate:
        return bool(gate["speed"])
    return unit in SPEED_UNITS or str(unit).endswith("/s")


def metric_speed(m, rec):
    """How many times as fast this host is as the recorded one, from v1.2.2's
    value of the metric (rates: now/recorded; times: recorded/now), or None."""
    a, b = rec.get("v1_2_2"), m["values"].get("v1_2_2")
    if a is None or b is None or a <= 0 or b <= 0:
        return None
    return b / a if m["better"] == "higher" else a / b


def median(xs):
    xs = sorted(xs)
    n = len(xs)
    return None if not n else xs[n // 2] if n % 2 else (xs[n // 2 - 1] + xs[n // 2]) / 2.0


def host_factors(results, baseline):
    """-> {"global": f|None, "groups": {"suite/config": f}, "counts": {...}}."""
    per_group, allf = {}, []
    for key, m in results.items():
        entry = baseline.get("metrics", {}).get(key)
        if not entry:
            continue
        gates = entry.get("gate") or m["gate"]
        gates = [gates] if isinstance(gates, dict) else gates
        if not is_speed_metric(m["unit"]) or any(g.get("speed") is False for g in gates):
            continue
        f = metric_speed(m, entry.get("recorded", {}))
        if f is not None:
            per_group.setdefault("%s/%s" % (m["suite"], m["config"]), []).append(f)
            allf.append(f)
    return {"global": median(allf), "n": len(allf),
            "groups": {g: median(v) for g, v in sorted(per_group.items())},
            "counts": {g: len(v) for g, v in sorted(per_group.items())}}


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


def check_gate(g, m, rec, host=None):
    """-> (status, limit_text). status: pass, FAIL, new, skip, info.

    host: {"mode": "anchor"|"suite"|"global", "speed": f} for a speed-dependent
    metric: a `rel` gate then judges against the recorded candidate value
    scaled to this host (see the module docstring)."""
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
        note = ""
        if host and host.get("speed") not in (None, 1.0):
            was = r
            r = r * host["speed"] if better == "higher" else r / host["speed"]
            note = ", recorded %s scaled by host speed %.2fx (%s)" % (fmt(was), host["speed"], host["mode"])
        elif host:
            note = ", host speed 1.00x (%s)" % host["mode"]
        if better == "lower":
            lim = r * (1 + g["pct"] / 100.0)
            parts = ["%g%% over %s" % (g["pct"], fmt(r))]
            if "abs" in g:
                lim = max(lim, r + g["abs"])
                parts.append("+%g" % g["abs"])
            if "redis_plus" in g and now.get("redis") is not None:
                lim = max(lim, now["redis"] + g["redis_plus"])
                parts.append("Redis %s + %g" % (fmt(now["redis"]), g["redis_plus"]))
            return ("pass" if cand <= lim else "FAIL"), "<= %s (%s%s)" % (fmt(lim), ", ".join(parts), note)
        lim = r * (1 - g["pct"] / 100.0)
        if "abs" in g:
            lim = min(lim, r - g["abs"])
        return ("pass" if cand >= lim else "FAIL"), ">= %s (%g%% under %s%s)" % (fmt(lim), g["pct"], fmt(r), note)
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


def host_for(g, m, rec, hf):
    """The host-speed scaling of one gate of one metric, or None."""
    if g.get("kind") != "rel" or not is_speed_metric(m["unit"], g):
        return None
    f = metric_speed(m, rec)
    if f is not None and g.get("anchor"):  # opt-in: this metric's v1.2.2 is a stable proxy
        return {"mode": "anchor", "speed": f}
    k = "%s/%s" % (m["suite"], m["config"])
    if hf["counts"].get(k, 0) >= MIN_GROUP:
        return {"mode": "suite", "speed": hf["groups"][k]}
    if hf["global"] is not None:
        return {"mode": "global", "speed": hf["global"]}
    return {"mode": "none", "speed": 1.0}


def evaluate(results, baseline, smoke=False):
    """-> (rows, not_run, host factors)."""
    hf = host_factors(results, baseline)
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
            checks = [(g,) + check_gate(g, m, rec, host_for(g, m, rec, hf)) for g in gates]
        sts = [c[1] for c in checks]
        status = "FAIL" if "FAIL" in sts else "pass" if "pass" in sts else \
            "new" if "new" in sts else "skip" if "skip" in sts else "smoke" if "smoke" in sts else "info"
        rows.append({"key": key, "suite": m["suite"], "config": m["config"], "metric": m["metric"],
                     "unit": m["unit"], "better": m["better"], "values": m["values"],
                     "samples": m["samples"], "recorded_candidate": rec.get("candidate"),
                     "host": next(({"mode": h["mode"], "speed": round(h["speed"], 4)}
                                   for h in [host_for(g, m, rec, hf) for g in gates] if h), None),
                     "gates": [{"gate": g, "status": s, "limit": t} for g, s, t in checks],
                     "status": status})
    not_run = sorted(set(baseline.get("metrics", {})) - set(results))
    return rows, not_run, hf


def host_warnings(hf):
    f = hf["global"]
    if f is None:
        return []
    if f < FACTOR_WARN_LOW:
        return ["WARN: this host is %.2fx as fast as the one the baseline was recorded on: absolute "
                "bounds (max ms) were recorded on a faster host and may fail without a regression" % f]
    if f > FACTOR_WARN_HIGH:
        return ["WARN: this host is %.2fx as fast as the one the baseline was recorded on: absolute "
                "bounds (max ms) were recorded on a slower host and may be too loose" % f]
    return []


def recorded_cpus(baseline):
    return sorted({m["recorded"]["cpu"] for m in baseline.get("metrics", {}).values()
                   if m.get("recorded", {}).get("cpu")})


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


def write_summary(outdir, rows, not_run, env, label, problems, smoke, hf=None, baseline=None):
    hf = hf or {"global": None, "groups": {}, "counts": {}, "n": 0}
    warns = host_warnings(hf)
    rec_cpus = recorded_cpus(baseline or {})
    fails = [r for r in rows if r["status"] == "FAIL"]
    verdict = "INVALID" if problems else "SMOKE" if smoke else "REGRESSED" if fails else "ok"
    summary = {"label": label, "env": env, "rows": rows, "not_run": not_run, "problems": problems,
               "failed": [r["key"] for r in fails], "verdict": verdict,
               "host_factor": {"global": hf["global"], "metrics": hf["n"], "suites": hf["groups"]},
               "recorded_cpu": rec_cpus, "warnings": warns}
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
    if warns:
        L += ["- %s" % w for w in warns] + [""]
    if env:
        L.append("- Machine: %s, %s vCPU, %s, kernel %s, glibc %s" % (
            env.get("vm", "?"), env.get("nproc", "?"), env.get("cpu", "?"), env.get("kernel", "?"), env.get("glibc", "?")))
        cur = env.get("cpu", "?")
        if rec_cpus:
            L.append("- CPU: recorded on %s; now %s%s" % (" / ".join(rec_cpus), cur,
                                                          "" if cur in rec_cpus else " (a different model: speed gates are session-relative)"))
        if env.get("calib_ms") is not None:
            L.append("- calibration (indicative only): %s ms for a fixed sha256 loop, %s cores" % (
                env["calib_ms"], env.get("nproc", "?")))
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
    if hf["global"] is not None:
        L += ["Host speed (this host vs the recorded one; 1.00 = same, below 1 = slower), from v1.2.2 "
              "measured in both sessions: global %.2fx over %d metrics" % (hf["global"], hf["n"])]
        L += ["- %s: %.2fx (%d metrics)" % (g, f, hf["counts"][g]) for g, f in hf["groups"].items()]
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
        rec.update({"vm": env.get("vm"), "cpu": env.get("cpu"), "date": date, "commit": env.get("candidate_commit"), "label": label})
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
    rows, not_run, hf = evaluate(results, baseline, smoke)
    summary = write_summary(a.results_dir, rows, not_run, env, label, problems, smoke, hf, baseline)
    for p in problems:
        print("INVALID: %s" % p)
    for w in summary["warnings"]:
        print(w)
    if hf["global"] is not None:
        print("host speed vs recorded: %.2fx (%d v1.2.2 metrics)" % (hf["global"], hf["n"]))
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
