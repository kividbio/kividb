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
             (REGRESS_REDIS_SHA256=<sha> accepts a deliberate Redis change),
             or the candidate was built by an older rustc than v1.2.2,
             or by one that cannot be read (REGRESS_ALLOW_RUSTC=1 accepts
             a deliberate check; a newer rustc only warns)
  SMOKE      SCALE != 1: gates are not checked, only that every       exit 0
             metric came out

Host speed. The same VM size can land on different CPU generations (two
Standard_D4s_v5 differed by ~40 %), so a `rel` gate on a speed-dependent metric
(unit ms, s, us, ns or */s; a gate's "speed": false/true overrides) is not
judged against the recorded absolute. v1.2.2 is measured in every session, so
the recorded candidate value is scaled to this host by an effective factor f
(rates x f, times / f; f < 1 = this host is slower):
  own  = the metric's own v1.2.2 now/recorded (None if v1.2.2 lacks it);
  grp  = the median of `own` over the suite/config's speed metrics (at least 3),
         else over the speed metrics recorded on the same machine (VM size
         and CPU model, at least 3: suites recorded on another host, or on
         a min_cpus suite's bigger machine, are not scaled by this one's),
         else over all suites (global); none at all = no scaling, said so;
  own is None                -> f = grp
  own < grp / 1.3            -> f = own   (this metric slowed more: loosens)
  own > grp * 1.3            -> f = min(own, max(grp, 1))  (never tightens
                                 beyond the recorded value on one noisy number)
  otherwise                  -> f = grp
A gate with "anchor": true always uses own, "anchor": false never. An own or
group factor used outside 0.4-2.5 makes the run INVALID (v1.2.2 starved or a
different host class: rerun). Bytes, counts, recall and ratios are never
scaled; `bound` gates stay absolute; a global factor under 0.7 or over 1.4 only
adds a WARN line.
"""
import argparse
import datetime
import json
import math
import os
import sys

ENG = ("v1_2_2", "candidate", "redis")

# The host-speed rules: see the module docstring.
SPEED_UNITS = {"ms", "s", "us", "ns"}
MIN_GROUP = 3  # speed metrics a suite/config needs for its own factor
TRUST = 1.3  # own vs group factor ratio beyond which the metric is treated on its own
FACTOR_MIN, FACTOR_MAX = 0.4, 2.5  # a used factor outside this: v1.2.2 is not trustworthy
FACTOR_WARN_LOW, FACTOR_WARN_HIGH = 0.7, 1.4


def speed_metric(unit, gates):
    """Does the metric's value move with the host's speed (times, rates)? A gate's
    "speed" key overrides the unit rule (False wins)."""
    vals = [g["speed"] for g in gates if "speed" in g]
    if vals:
        return all(vals)
    return unit in SPEED_UNITS or str(unit).endswith("/s")


def is_speed_metric(unit, gate=None):
    return speed_metric(unit, [gate] if gate else [])


def usable(x):
    return isinstance(x, (int, float)) and not isinstance(x, bool) and math.isfinite(x) and x > 0


def metric_speed(m, rec):
    """How many times as fast this host is as the recorded one, from v1.2.2's
    value of the metric (rates: now/recorded; times: recorded/now), or None."""
    a, b = rec.get("v1_2_2"), m["values"].get("v1_2_2")
    if not usable(a) or not usable(b):
        return None
    return b / a if m["better"] == "higher" else a / b


def median(xs):
    xs = sorted(xs)
    n = len(xs)
    return None if not n else xs[n // 2] if n % 2 else (xs[n // 2 - 1] + xs[n // 2]) / 2.0


def gates_of(entry, m):
    gates = (entry or {}).get("gate") or m["gate"]
    return [gates] if isinstance(gates, dict) else gates


def host_factors(results, baseline):
    """-> {"global": f|None, "groups": {"suite/config": f}, "counts": {...}}."""
    per_group, per_host, allf = {}, {}, []
    for key, m in results.items():
        entry = baseline.get("metrics", {}).get(key)
        if not entry or not speed_metric(m["unit"], gates_of(entry, m)):
            continue
        rec = entry.get("recorded", {})
        f = metric_speed(m, rec)
        if f is not None:
            per_group.setdefault("%s/%s" % (m["suite"], m["config"]), []).append(f)
            per_host.setdefault(recorded_host(rec), []).append(f)
            allf.append(f)
    return {"global": median(allf), "n": len(allf),
            "groups": {g: median(v) for g, v in sorted(per_group.items())},
            "counts": {g: len(v) for g, v in sorted(per_group.items())},
            "hosts": {h: median(v) for h, v in sorted(per_host.items())},
            "host_counts": {h: len(v) for h, v in sorted(per_host.items())}}


def recorded_host(rec):
    """The machine a metric's values were recorded on: VM size and CPU model.
    A suite/config with too few speed metrics of its own is scaled by the
    factor of the metrics recorded on the same machine, not by all of them:
    suites recorded on another CPU (or on the bigger machine of a min_cpus
    suite) would otherwise be scaled by a factor that says how the OTHER
    host compares (#921: suite 25, recorded on this very CPU, judged at the
    1.17x of the suites recorded on an older one)."""
    return "%s / %s" % (rec.get("vm") or "?", rec.get("cpu") or "?")


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

    host: {"mode": "anchor"|"suite"|"host"|"global", "speed": f} for a speed-dependent
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
            if r - g["abs"] <= 0:
                return "INVALID", "configuration error: abs %g leaves no limit under the recorded %s%s" % (
                    g["abs"], fmt(r), note)
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
    if kind == "vs_v1_2_2":
        old = now.get("v1_2_2")
        if old is None or old <= 0:
            return "skip", "vs_v1_2_2: v1.2.2 not measured this run"
        x = cand / old if better == "lower" else old / cand if cand > 0 else float("inf")
        return ("pass" if x <= g["max_x"] else "FAIL"), "%.2fx v1.2.2 <= %gx" % (x, g["max_x"])
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


def host_for(g, m, rec, hf, gates=None):
    """The host-speed scaling of one gate of one metric, or None.
    -> {"mode": rule, "speed": f, "own": own|None, "group": grp|None}"""
    gates = gates or [g]
    if g.get("kind") != "rel" or not speed_metric(m["unit"], gates):
        return None
    own = metric_speed(m, rec)
    k = "%s/%s" % (m["suite"], m["config"])
    hk = recorded_host(rec)
    if hf["counts"].get(k, 0) >= MIN_GROUP:
        grp, grule = hf["groups"][k], "group"
    elif len(hf.get("hosts", {})) > 1 and hf["host_counts"].get(hk, 0) >= MIN_GROUP:
        # (With one recording host its factor is the global one.)
        grp, grule = hf["hosts"][hk], "host"
    elif hf["global"] is not None:
        grp, grule = hf["global"], "global"
    else:
        grp, grule = None, "none"

    def out(rule, f):
        return {"mode": rule, "speed": f, "own": own, "group": grp}
    if g.get("anchor") is True and own is not None:
        return out("anchor", own)
    if grp is None:
        return out("none", 1.0)
    if own is None or g.get("anchor") is False:
        return out(grule, grp)
    if own < grp / TRUST:
        return out("own-slower", own)
    if own > grp * TRUST:
        return out("own-capped", min(own, max(grp, 1.0)))
    return out(grule, grp)


def evaluate(results, baseline, smoke=False):
    """-> (rows, not_run, host factors). hf also has "problems" (INVALID reasons
    found while judging), "differ" and "unscaled"."""
    hf = host_factors(results, baseline)
    hf["problems"], hf["differ"], hf["unscaled"] = [], [], []
    rows = []
    for key in sorted(results):
        m = results[key]
        entry = baseline.get("metrics", {}).get(key)
        rec = (entry or {}).get("recorded", {})
        gates = gates_of(entry, m)
        hosts = [host_for(g, m, rec, hf, gates) for g in gates]
        host = next((h for h in hosts if h), None)
        if smoke:  # SCALE != 1: the numbers mean nothing
            checks = [(g, "smoke", "") for g in gates]
        else:
            checks = [(g,) + check_gate(g, m, rec, h) for g, h in zip(gates, hosts)]
            if host:
                bad = [("this metric's", host["own"])] if host["mode"] in ("own-slower", "anchor") else []
                if host["group"] is not None:
                    bad.append(("the %s" % ({"global": "global", "host": "recording host's"}.get(
                        host["mode"], "suite/config's")), host["group"]))
                for what, f in bad:
                    if f is not None and not (FACTOR_MIN <= f <= FACTOR_MAX):
                        hf["problems"].append(
                            "%s: v1.2.2 measured %.2fx of the recorded speed (%s factor; allowed %g-%g): "
                            "the anchor is not trustworthy, rerun" % (key, f, what, FACTOR_MIN, FACTOR_MAX))
                if host["mode"] == "none":
                    hf["unscaled"].append(key)
                if host["own"] is not None and host["group"] is not None and \
                        not (1 / TRUST <= host["own"] / host["group"] <= TRUST):
                    hf["differ"].append({"key": key, "own": round(host["own"], 4),
                                         "group": round(host["group"], 4), "rule": host["mode"]})
            for g, st, t in checks:
                if st == "INVALID":
                    hf["problems"].append("%s: %s" % (key, t))
        sts = [c[1] for c in checks]
        status = "FAIL" if "FAIL" in sts else "INVALID" if "INVALID" in sts else "pass" if "pass" in sts else \
            "new" if "new" in sts else "skip" if "skip" in sts else "smoke" if "smoke" in sts else "info"
        rows.append({"key": key, "suite": m["suite"], "config": m["config"], "metric": m["metric"],
                     "unit": m["unit"], "better": m["better"], "values": m["values"],
                     "samples": m["samples"], "recorded_candidate": rec.get("candidate"),
                     "host": None if not host else {
                         "mode": host["mode"], "speed": round(host["speed"], 4),
                         "own": None if host["own"] is None else round(host["own"], 4),
                         "group": None if host["group"] is None else round(host["group"], 4)},
                     "gates": [{"gate": g, "status": s, "limit": t} for g, s, t in checks],
                     "status": status})
    not_run = sorted(set(baseline.get("metrics", {})) - set(results))
    hf["problems"] = sorted(set(hf["problems"]))
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


RUSTC_OVERRIDE = "REGRESS_ALLOW_RUSTC"


def rustc_version(v):
    """'1.100.0' (or '1.101.0-nightly') -> (1, 100, 0); None if unknown."""
    if not v:
        return None
    parts = v.split("-", 1)[0].split(".")
    try:
        return tuple(int(p) for p in parts)
    except ValueError:
        return None


def rustc_check(env, override=None):
    """(problems, warnings) about the compilers of the candidate and of v1.2.2.

    The same code built by rustc 1.99.1 ran script and PUBLISH rows 10-30 %
    slower than built by 1.100.0, which v1.2.2's release binaries were built
    by (#900): a gate against v1.2.2 then compares compilers, not code. A
    candidate built by an OLDER rustc than v1.2.2, or by one that cannot be
    read from the binary, makes the run INVALID; a newer one (the normal case:
    rust-toolchain.toml says `stable`) only warns. REGRESS_ALLOW_RUSTC=1 turns
    the INVALID cases into warnings, for a deliberate check of the compiler's
    effect (results/pr900-rustc199-check)."""
    if override is None:
        override = os.environ.get(RUSTC_OVERRIDE, "") not in ("", "0") or bool((env or {}).get("rustc_override"))
    engines = (env or {}).get("engines", {})
    selected = (env or {}).get("engines_selected") or list(ENG)
    probs, warns = [], []
    if "candidate" not in selected or "v1_2_2" not in selected:
        return probs, warns
    for ref, cand in (("v1_2_2", "candidate"), ("v1_2_2_full", "candidate_full")):
        if not engines.get(ref) or not engines.get(cand):
            continue
        a, b = engines[ref].get("rustc"), engines[cand].get("rustc")
        va, vb = rustc_version(a), rustc_version(b)
        if vb is None or va is None:
            msg = "%s was built by rustc %s and %s by rustc %s: unknown, so its rows against v1.2.2 may " \
                  "compare compilers as well as code" % (cand, b or "unknown", ref, a or "unknown")
            (warns if override else probs).append(("WARN: " if override else "") + msg)
        elif vb < va:
            msg = "%s was built by rustc %s, older than %s's %s: its rows against v1.2.2 compare compilers " \
                  "as well as code (build it with rustc %s or later, or set %s=1 for a deliberate check)" % (
                      cand, b, ref, a, a, RUSTC_OVERRIDE)
            (warns if override else probs).append(("WARN: " if override else "") + msg)
        elif vb > va:
            warns.append("WARN: %s was built by rustc %s and %s by rustc %s: a newer compiler may account for "
                         "part of a difference against v1.2.2" % (cand, b, ref, a))
    return probs, warns


def toolchain_warnings(env):
    return rustc_check(env)[1]


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
    # The machine size the values were recorded on, per metric that ran: a
    # suite with min_cpus is recorded on a bigger machine than the others.
    if not smoke and not allow_vm_change:
        wrong = {}
        for k in results:
            vm = metrics.get(k, {}).get("recorded", {}).get("vm")
            if vm and vm != env.get("vm"):
                wrong.setdefault(vm, set()).add(k.split("/", 1)[0])
        for vm, suites in sorted(wrong.items()):
            probs.append("VM size %r differs from the %s that %s %s recorded on" % (
                env.get("vm"), vm, ", ".join(sorted(suites)), "was" if len(suites) == 1 else "were"))
    # A suite that needs more CPUs than this machine has, run because --only
    # named it: its numbers are the load generator's, not the server's.
    for s, n in sorted((env.get("ran_below_min_cpus") or {}).items()):
        probs.append("%s needs %s CPUs and ran on %s: not valid for the gate (run it with --min-cpus-only "
                     "on a machine that big)" % (s, n, env.get("nproc", "?")))
    # The reference binaries the values were recorded with.
    pins = baseline.get("pins", {})
    engines = env.get("engines", {})
    selected = env.get("engines_selected") or list(ENG)
    want = pins.get("v1_2_2_sha256")
    got = engines.get("v1_2_2", {}).get("sha256")
    if want and "v1_2_2" in selected and got != want:
        probs.append("v1.2.2 binary sha256 %s is not the pinned %s" % (got, want))
    want = pins.get("v1_2_2_full_sha256")
    got = engines.get("v1_2_2_full", {}).get("sha256")
    if want and "v1_2_2" in selected and got and got != want:
        probs.append("v1.2.2 full binary sha256 %s is not the pinned %s" % (got, want))
    probs += rustc_check(env)[0]
    want = os.environ.get("REGRESS_REDIS_SHA256") or pins.get("redis_sha256")
    got = engines.get("redis", {}).get("sha256")
    # A Redis built on this machine from the pinned release tarball (CI) has
    # its own binary hash: then the source's is the pin, the binary's is shown.
    src_ok = bool(env.get("redis_source_sha256")) and env.get("redis_source_sha256") == pins.get("redis_source_sha256")
    if want and "redis" in selected and got != want and not src_ok:
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
    warns = host_warnings(hf) + toolchain_warnings(env)
    rec_cpus = recorded_cpus(baseline or {})
    fails = [r for r in rows if r["status"] == "FAIL"]
    verdict = "INVALID" if problems else "SMOKE" if smoke else "REGRESSED" if fails else "ok"
    summary = {"label": label, "env": env, "rows": rows, "not_run": not_run, "problems": problems,
               "failed": [r["key"] for r in fails], "verdict": verdict,
               "host_factor": {"global": hf["global"], "metrics": hf["n"], "suites": hf["groups"]},
               "recorded_cpu": rec_cpus, "warnings": warns,
               "own_vs_group": hf.get("differ", []), "unscaled": hf.get("unscaled", [])}
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
    if hf.get("unscaled"):
        L += ["NOTE: v1.2.2 has no usable value for the speed metrics of %d gate(s): speed gates were compared "
              "absolutely against the recorded values (%s)." % (
                  len(hf["unscaled"]), ", ".join(hf["unscaled"][:5]) + (", ..." if len(hf["unscaled"]) > 5 else "")), ""]
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
        for e in ENG + ("v1_2_2_full", "candidate_full"):
            d = env.get("engines", {}).get(e)
            if d:
                L.append("- %s: `%s`, sha256 `%s`%s" % (e, d.get("version", "?"), d.get("sha256", "?"),
                                                       ", rustc %s" % d["rustc"] if d.get("rustc") else ""))
        if env.get("skipped_configs"):
            L.append("- skipped (need --candidate-full): %s" % ", ".join(env["skipped_configs"]))
        if env.get("needs_more_cpus"):
            L.append("- not run, need more CPUs than this machine's %s (run them with --min-cpus-only on one "
                     "that big): %s" % (env.get("nproc", "?"), ", ".join(
                         "%s (%s)" % kv for kv in sorted(env["needs_more_cpus"].items()))))
        if env.get("min_cpus_only"):
            L.append("- --min-cpus-only: just the suites that declare min_cpus")
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
        L += ["- %s: %.2fx (%d metrics%s)" % (g, f, hf["counts"][g], "" if hf["counts"][g] >= MIN_GROUP
                                              else ", too few: the recording host's factor is used")
              for g, f in hf["groups"].items()]
        L += ["- recorded on %s: %.2fx (%d metrics)" % (h, f, hf["host_counts"][h])
              for h, f in hf.get("hosts", {}).items()]
        L.append("")
        if hf.get("differ"):
            L += ["Metrics whose own v1.2.2 factor differs from their group's by more than %gx "
                  "(rule applied in brackets):" % TRUST, ""]
            L += ["- %s: own %.2fx, group %.2fx (%s)" % (d["key"], d["own"], d["group"], d["rule"]) for d in hf["differ"]]
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
        # The compiler of the candidate that made these values (#900).
        eng_env = env.get("engines", {})
        rustc = (eng_env.get("candidate") or {}).get("rustc")
        rustc_full = (eng_env.get("candidate_full") or {}).get("rustc")
        if rustc:
            rec["rustc"] = rustc
        if rustc_full:
            rec["rustc_full"] = rustc_full
        if env.get("nproc"):
            rec["nproc"] = env["nproc"]
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
    if eng.get("v1_2_2_full", {}).get("sha256"):
        pins.setdefault("v1_2_2_full_sha256", eng["v1_2_2_full"]["sha256"])
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
    problems = problems + hf["problems"]
    summary = write_summary(a.results_dir, rows, not_run, env, label, problems, smoke, hf, baseline)
    for p in problems:
        print("INVALID: %s" % p)
    for w in summary["warnings"]:
        print(w)
    if hf["unscaled"]:
        print("NOTE: v1.2.2 has no usable value for %d speed gate(s): compared absolutely" % len(hf["unscaled"]))
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
