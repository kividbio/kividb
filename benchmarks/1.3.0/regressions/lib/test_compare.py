#!/usr/bin/env python3
"""Self-test of compare.py's host-speed logic; runs anywhere: python lib/test_compare.py

Synthetic baseline: suite s/c with three speed metrics that 1.2.2 also measures
(a rate and two times), a rate 1.2.2 lacks (new_rate), and a memory metric.
A host that is `speed` times as fast makes rates x speed and times / speed.
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import compare  # noqa: E402

REL = [{"kind": "rel", "pct": 10}]
SPEC = [  # metric, unit, better, v1.2.2, candidate
    ("qps", "q/s", "higher", 1000.0, 1500.0),
    ("t1_ms", "ms", "lower", 10.0, 8.0),
    ("t2_s", "s", "lower", 100.0, 40.0),
    ("new_rate", "req/s", "higher", None, 500.0),
    ("mem_mib", "MiB", "lower", 100.0, 90.0),
]


def baseline():
    ms = {}
    for name, unit, better, v, c in SPEC:
        ms["s/c/" + name] = {"suite": "s", "config": "c", "metric": name, "unit": unit, "better": better,
                             "recorded": {"v1_2_2": v, "candidate": c, "redis": None, "vm": "X"}, "gate": REL}
    return {"metrics": ms}


def run(speed, cand_factor=None, mem=None):
    """Results of a host `speed` times as fast; cand_factor[name] multiplies a
    candidate's value on top (a real change)."""
    cand_factor = cand_factor or {}
    res = {}
    for name, unit, better, v, c in SPEC:
        k = 1.0 if unit == "MiB" else speed if better == "higher" else 1.0 / speed
        vals = {"candidate": c * k * cand_factor.get(name, 1.0)}
        if v is not None:
            vals["v1_2_2"] = v * k
        if name == "mem_mib" and mem is not None:
            vals["candidate"] = mem
        res["s/c/" + name] = {"suite": "s", "config": "c", "metric": name, "unit": unit, "better": better,
                              "gate": REL, "values": vals, "samples": {}}
    return res


def verdict(results):
    rows, _, hf = compare.evaluate(results, baseline())
    return {r["metric"]: r["status"] for r in rows}, hf


class HostSpeed(unittest.TestCase):
    def test_same_host_passes(self):
        st, hf = verdict(run(1.0))
        self.assertTrue(all(s == "pass" for s in st.values()), st)
        self.assertAlmostEqual(hf["global"], 1.0)

    def test_uniformly_slower_host_passes(self):
        st, hf = verdict(run(0.6))
        self.assertTrue(all(s == "pass" for s in st.values()), st)
        self.assertAlmostEqual(hf["global"], 0.6)
        self.assertEqual(compare.host_warnings(hf)[0][:5], "WARN:")

    def test_faster_host_cannot_hide_a_regression(self):
        st, _ = verdict(run(1.5, {"qps": 0.6}))  # candidate 40 % down on a host 1.5x as fast
        self.assertEqual(st["qps"], "FAIL")

    def test_slower_host_and_candidate_lost_30_percent(self):
        st, _ = verdict(run(0.6, {"qps": 0.7}))
        self.assertEqual(st["qps"], "FAIL")
        self.assertEqual(st["t1_ms"], "pass")
        st, _ = verdict(run(0.6, {"t1_ms": 1.3}))
        self.assertEqual(st["t1_ms"], "FAIL")

    def test_metric_without_v122_uses_the_host_factor(self):
        st, _ = verdict(run(0.6))
        self.assertEqual(st["new_rate"], "pass")  # absolute 500 * 0.6 would fail the recorded value
        st, _ = verdict(run(0.6, {"new_rate": 0.7}))
        self.assertEqual(st["new_rate"], "FAIL")
        # without the factor the unscaled recorded 500 would have been the limit
        rows, _, _ = compare.evaluate(run(0.6), baseline())
        row = [r for r in rows if r["metric"] == "new_rate"][0]
        self.assertEqual(row["host"]["mode"], "group")

    def test_memory_is_never_scaled(self):
        st, _ = verdict(run(0.6, mem=105.0))  # 90 recorded, 10 % -> 99: must fail on a slow host
        self.assertEqual(st["mem_mib"], "FAIL")
        st, _ = verdict(run(1.6, mem=105.0))
        self.assertEqual(st["mem_mib"], "FAIL")
        rows, _, _ = compare.evaluate(run(0.6), baseline())
        self.assertIsNone([r for r in rows if r["metric"] == "mem_mib"][0]["host"])

    def test_global_fallback_when_suite_has_no_v122_speed_metric(self):
        b = baseline()
        res = run(0.6)
        for name in ("qps", "t1_ms", "t2_s"):
            b["metrics"]["s/c/" + name]["recorded"]["v1_2_2"] = None
        for name in ("new_rate",):
            pass
        # another suite supplies the global factor
        for i in range(3):
            k = "o/c/t%d_ms" % i
            b["metrics"][k] = {"suite": "o", "config": "c", "metric": "t%d_ms" % i, "unit": "ms",
                               "better": "lower", "recorded": {"v1_2_2": 10.0, "candidate": 5.0}, "gate": REL}
            res[k] = {"suite": "o", "config": "c", "metric": "t%d_ms" % i, "unit": "ms", "better": "lower",
                      "gate": REL, "values": {"v1_2_2": 10 / 0.6, "candidate": 5 / 0.6}, "samples": {}}
        rows, _, hf = compare.evaluate(res, b)
        row = [r for r in rows if r["metric"] == "qps"][0]
        self.assertEqual(row["host"]["mode"], "global")
        self.assertEqual(row["status"], "pass")
        self.assertAlmostEqual(hf["global"], 0.6)

    def test_bound_gates_stay_absolute(self):
        m = {"suite": "s", "config": "c", "metric": "p_ms", "unit": "ms", "better": "lower",
             "values": {"candidate": 25.0}}
        self.assertEqual(compare.check_gate({"kind": "bound", "max": 20}, m, {})[0], "FAIL")

    def test_speed_override_and_units(self):
        self.assertTrue(compare.is_speed_metric("ms"))
        self.assertTrue(compare.is_speed_metric("VADD/s"))
        self.assertFalse(compare.is_speed_metric("B"))
        self.assertFalse(compare.is_speed_metric("ratio"))
        self.assertFalse(compare.is_speed_metric("ms", {"kind": "rel", "speed": False}))
        self.assertTrue(compare.is_speed_metric("ratio", {"kind": "rel", "speed": True}))

class RealSlowHost(unittest.TestCase):
    """PR #799's runs (simd-9501caa6-d4s_v5-09): a D4s_v5 whose VSIM was ~40 %
    slower than the recorded one, while its VADD was not."""

    def test_f32_vsim_on_the_slow_host(self):
        rows = [  # metric, unit, better, rec v1.2.2, rec candidate, now v1.2.2, now candidate
            ("vadd_per_s", "VADD/s", "higher", 66395.4, 62037.9, 65358.5, 59838.8),
            ("vsim_qps", "q/s", "higher", 1123.9, 1115.5, 639.1, 616.2),
            ("vsim_p50_ms", "ms", "lower", 1.853, 1.908, 3.2, 3.3)]
        b, res = {"metrics": {}}, {}
        for n, u, bt, rv, rc, nv, nc in rows:
            k = "09/f32/" + n
            g = [{"kind": "rel", "pct": 25}]
            b["metrics"][k] = {"suite": "09", "config": "f32", "metric": n, "unit": u, "better": bt,
                               "recorded": {"v1_2_2": rv, "candidate": rc}, "gate": g}
            res[k] = {"suite": "09", "config": "f32", "metric": n, "unit": u, "better": bt, "gate": g,
                      "values": {"v1_2_2": nv, "candidate": nc}, "samples": {}}
        r, _, hf = compare.evaluate(res, b)
        self.assertTrue(all(x["status"] == "pass" for x in r), [x["status"] for x in r])
        self.assertAlmostEqual(hf["groups"]["09/f32"], 0.579, places=2)
        res["09/f32/vsim_qps"]["values"]["candidate"] = 616.2 * 0.7  # a real loss on top
        r, _, _ = compare.evaluate(res, b)
        self.assertEqual([x["status"] for x in r if x["metric"] == "vsim_qps"], ["FAIL"])


def build(specs):
    """specs: (suite/config, name, unit, better, rec v1.2.2, rec cand, now v1.2.2, now cand[, gates])."""
    b, res = {"metrics": {}}, {}
    for sp in specs:
        sc, n, u, bt, rv, rc, nv, nc = sp[:8]
        g = sp[8] if len(sp) > 8 else REL
        suite, config = sc.split("/")
        k = "%s/%s" % (sc, n)
        b["metrics"][k] = {"suite": suite, "config": config, "metric": n, "unit": u, "better": bt,
                           "recorded": {"v1_2_2": rv, "candidate": rc}, "gate": g}
        res[k] = {"suite": suite, "config": config, "metric": n, "unit": u, "better": bt, "gate": g,
                  "values": {"v1_2_2": nv, "candidate": nc}, "samples": {}}
    return b, res


def status(specs):
    b, res = build(specs)
    rows, _, hf = compare.evaluate(res, b)
    return {r["metric"]: r["status"] for r in rows}, rows, hf


def steady(sc, n=3, speed=1.0):
    """n healthy time metrics of a suite/config on a host `speed` times as fast."""
    return [(sc, "t%d_ms" % i, "ms", "lower", 10.0, 8.0, 10.0 / speed, 8.0 / speed) for i in range(n)]


class Rules(unittest.TestCase):
    def test_uniform_candidate_regression_on_same_speed_host_fails(self):
        # a factor computed from candidate values would absorb this; v1.2.2 is flat
        specs = [("s/c", "r%d" % i, "q/s", "higher", 100.0, 150.0, 100.0, 150.0 * 0.6) for i in range(3)]
        specs += [("s/c", "t%d" % i, "ms", "lower", 10.0, 8.0, 10.0, 8.0 / 0.6) for i in range(3)]
        st, _, hf = status(specs)
        self.assertTrue(all(v == "FAIL" for v in st.values()), st)
        self.assertAlmostEqual(hf["global"], 1.0)

    def test_bimodal_host_vadd_collapse_fails_healthy_passes(self):
        def rows(vadd):
            return [("09/f32", "vadd_per_s", "VADD/s", "higher", 66395.4, 62037.9, 65358.5, vadd),
                    ("09/f32", "vsim_qps", "q/s", "higher", 1123.9, 1115.5, 639.1, 616.2),
                    ("09/f32", "vsim_p50_ms", "ms", "lower", 1.853, 1.908, 3.2, 3.3)]
        st, r, _ = status(rows(59838.8))
        self.assertEqual(st["vadd_per_s"], "pass")
        self.assertEqual([x["host"]["mode"] for x in r if x["metric"] == "vadd_per_s"], ["own-capped"])
        st, _, _ = status(rows(27000.0))
        self.assertEqual(st["vadd_per_s"], "FAIL")
        self.assertEqual(st["vsim_qps"], "pass")

    def test_load_time_healthy_on_slower_cpu_with_flat_pings(self):
        jit = [{"kind": "rel", "pct": 50, "redis_plus": 10, "speed": False}]
        specs = steady("02/c", 2)  # two metrics that did not slow
        specs.append(("02/c", "load_s", "s", "lower", 100.0, 40.0, 100.0 / 0.6, 40.0 / 0.6))
        specs += [("02/c", "ping_p99_ms", "ms", "lower", 5.0, 3.0, 5.0, 3.0, jit),
                  ("02/c", "ping_max_ms", "ms", "lower", 9.0, 6.0, 9.0, 6.0, jit)]
        st, rows, hf = status(specs)
        self.assertEqual(st["load_s"], "pass")
        self.assertEqual([r["host"]["mode"] for r in rows if r["metric"] == "load_s"], ["own-slower"])
        self.assertEqual(hf["counts"]["02/c"], 3)  # jitter rows do not count
        self.assertIsNone([r for r in rows if r["metric"] == "ping_p99_ms"][0]["host"])
        self.assertTrue(any(d["key"] == "02/c/load_s" for d in hf["differ"]))
        # and it still fails when load_s really regressed
        specs[2] = specs[2][:7] + (40.0 / 0.6 * 1.3,)
        self.assertEqual(status(specs)[0]["load_s"], "FAIL")

    def test_noisy_own_factor_does_not_tighten(self):
        # reshard: v1.2.2 a different code path, own 1.43 against a group at 1.0
        specs = steady("13/c", 3) + [("13/c", "reshard_s", "s", "lower", 381.0, 25.3, 381.0 / 1.43, 27.02,
                                      [{"kind": "rel", "pct": 15}])]
        st, rows, _ = status(specs)
        self.assertEqual(st["reshard_s"], "pass")
        row = [r for r in rows if r["metric"] == "reshard_s"][0]
        self.assertEqual((row["host"]["mode"], row["host"]["speed"]), ("own-capped", 1.0))

    def test_anchor_flags(self):
        base = steady("s/c", 3)
        odd = ("s/c", "x_s", "s", "lower", 100.0, 40.0, 100.0 / 1.5, 40.0 / 1.5)
        for flag, mode in ((True, "anchor"), (False, "group")):
            _, rows, _ = status(base + [odd + ([{"kind": "rel", "pct": 10, "anchor": flag}],)])
            self.assertEqual([r["host"]["mode"] for r in rows if r["metric"] == "x_s"], [mode])

    def test_starved_v122_is_invalid(self):
        specs = [("s/c", "r%d" % i, "q/s", "higher", 100.0, 150.0, 5.0, 150.0) for i in range(3)]
        _, _, hf = status(specs)
        self.assertTrue(hf["problems"] and "not trustworthy" in hf["problems"][0], hf["problems"])
        # end to end: exit status 1
        import json
        import tempfile
        b, res = build(specs)
        with tempfile.TemporaryDirectory() as d:
            with open(os.path.join(d, "results.jsonl"), "w") as f:
                for k, m in res.items():
                    for e, v in m["values"].items():
                        f.write(json.dumps({"suite": m["suite"], "config": m["config"], "metric": m["metric"],
                                            "unit": m["unit"], "better": m["better"], "gate": m["gate"],
                                            "engine": e, "value": v}) + "\n")
            with open(os.path.join(d, "env.json"), "w") as f:
                json.dump({"vm": "X", "scale": "1", "engines_selected": ["v1_2_2", "candidate"],
                           "expected": []}, f)
            with open(os.path.join(d, "baseline.json"), "w") as f:
                json.dump(b, f)
            old = sys.argv
            sys.argv = ["compare.py", d, "--baseline", os.path.join(d, "baseline.json")]
            try:
                self.assertEqual(compare.main(), 1)
            finally:
                sys.argv = old
            self.assertEqual(json.load(open(os.path.join(d, "summary.json")))["verdict"], "INVALID")

    def test_unusable_v122_values(self):
        for bad in (float("nan"), 0.0, None, -3.0, float("inf")):
            specs = steady("s/c", 3) + [("s/c", "x_ms", "ms", "lower", bad, 8.0, bad, 8.0)]
            st, rows, hf = status(specs)
            self.assertEqual(st["x_ms"], "pass", bad)
            self.assertEqual(hf["counts"]["s/c"], 3, bad)
            self.assertEqual([r["host"]["mode"] for r in rows if r["metric"] == "x_ms"], ["group"])

    def test_no_v122_at_all_is_unscaled_and_said_so(self):
        specs = [("s/c", "x_ms", "ms", "lower", None, 8.0, None, 8.0)]
        st, rows, hf = status(specs)
        self.assertEqual(rows[0]["host"]["mode"], "none")
        self.assertEqual(hf["unscaled"], ["s/c/x_ms"])

    def test_group_needs_three_metrics(self):
        # A has 2 metrics on a host 0.6x as fast, B 3 on a same-speed host: a metric of A without
        # v1.2.2 must use the global factor (1.0), so its slow value fails; with a group
        # rule of 2 it would use 0.6 and pass.
        specs = steady("a/c", 2, 0.6) + steady("b/c", 3, 1.0)
        specs.append(("a/c", "new_rate", "req/s", "higher", None, 500.0, None, 300.0))
        st, rows, hf = status(specs)
        self.assertEqual(compare.MIN_GROUP, 3)
        self.assertEqual([r["host"]["mode"] for r in rows if r["metric"] == "new_rate"], ["global"])
        self.assertEqual(st["new_rate"], "FAIL")

    def test_abs_slack_lower_is_better(self):
        gate = [{"kind": "rel", "pct": 10, "abs": 5}]
        ok = status([("s/c", "x_ms", "ms", "lower", 10.0, 8.0, 10.0, 12.9, gate)])[0]
        bad = status([("s/c", "x_ms", "ms", "lower", 10.0, 8.0, 10.0, 13.5, gate)])[0]
        self.assertEqual((ok["x_ms"], bad["x_ms"]), ("pass", "FAIL"))  # limit max(8.8, 8 + 5) = 13

    def test_higher_abs_larger_than_recorded_is_invalid(self):
        gate = [{"kind": "rel", "pct": 10, "abs": 50}]
        st, _, hf = status([("s/c", "x_rps", "req/s", "higher", None, 40.0, None, 40.0, gate)])
        self.assertEqual(st["x_rps"], "INVALID")
        self.assertTrue(hf["problems"])

    def test_classification_agrees(self):
        # host_factors and host_for must both honour a gate's speed override
        gate = [{"kind": "rel", "pct": 10, "speed": False}]
        _, rows, hf = status(steady("s/c", 3) + [("s/c", "x_ms", "ms", "lower", 10.0, 8.0, 20.0, 8.0, gate)])
        self.assertEqual(hf["counts"]["s/c"], 3)
        self.assertIsNone([r for r in rows if r["metric"] == "x_ms"][0]["host"])
        gate = [{"kind": "rel", "pct": 10, "speed": True}]
        _, rows, hf = status(steady("s/c", 3) + [("s/c", "x_pct", "pct", "lower", 10.0, 8.0, 10.0, 8.0, gate)])
        self.assertEqual(hf["counts"]["s/c"], 4)
        self.assertIsNotNone([r for r in rows if r["metric"] == "x_pct"][0]["host"])



def rustc_env(cand, cand_full="1.100.0", ref="1.100.0", **extra):
    env = {"engines": {"v1_2_2": {"rustc": ref}, "candidate": {"rustc": cand},
                       "v1_2_2_full": {"rustc": ref}, "candidate_full": {"rustc": cand_full},
                       "redis": {"rustc": None}},
           "engines_selected": ["v1_2_2", "candidate", "redis"]}
    env.update(extra)
    return env


class Rustc(unittest.TestCase):
    """#900: a candidate built by another rustc than v1.2.2's."""

    def setUp(self):
        self._saved = os.environ.pop(compare.RUSTC_OVERRIDE, None)

    def tearDown(self):
        os.environ.pop(compare.RUSTC_OVERRIDE, None)
        if self._saved is not None:
            os.environ[compare.RUSTC_OVERRIDE] = self._saved

    def problems(self, env):
        return compare.validity_problems(env, {"metrics": {}}, {"x": {}}, False, False)

    def test_same_rustc_is_silent(self):
        probs, warns = compare.rustc_check(rustc_env("1.100.0"))
        self.assertEqual((probs, warns), ([], []))
        self.assertFalse(any("rustc" in p for p in self.problems(rustc_env("1.100.0"))))

    def test_an_older_rustc_is_invalid(self):
        probs, _ = compare.rustc_check(rustc_env("1.99.1"))
        self.assertEqual(len(probs), 1)
        self.assertIn("candidate was built by rustc 1.99.1, older than v1_2_2's 1.100.0", probs[0])
        self.assertTrue(any("older than" in p for p in self.problems(rustc_env("1.99.1"))))
        # The full pair is judged the same way.
        probs, _ = compare.rustc_check(rustc_env("1.100.0", cand_full="1.99.1"))
        self.assertIn("candidate_full", probs[0])

    def test_an_unknown_rustc_is_invalid(self):
        probs, _ = compare.rustc_check(rustc_env(None))
        self.assertEqual(len(probs), 1)
        self.assertIn("candidate was built by rustc unknown", probs[0])
        probs, _ = compare.rustc_check(rustc_env("garbage"))
        self.assertEqual(len(probs), 1)

    def test_a_newer_rustc_only_warns(self):
        probs, warns = compare.rustc_check(rustc_env("1.101.0"))
        self.assertEqual(probs, [])
        self.assertEqual(len(warns), 1)
        self.assertIn("newer compiler", warns[0])
        # Compared as numbers, not strings: 1.100 is newer than 1.99.
        self.assertEqual(compare.rustc_check(rustc_env("1.100.0", ref="1.99.1"))[0], [])

    def test_the_override_turns_invalid_into_a_warning(self):
        os.environ[compare.RUSTC_OVERRIDE] = "1"
        probs, warns = compare.rustc_check(rustc_env("1.99.1"))
        self.assertEqual(probs, [])
        self.assertTrue(warns and warns[0].startswith("WARN:"))
        del os.environ[compare.RUSTC_OVERRIDE]
        # Or recorded in env.json by run-all.sh.
        probs, warns = compare.rustc_check(rustc_env(None, rustc_override=True))
        self.assertEqual(probs, [])
        self.assertEqual(len(warns), 1)

    def test_no_candidate_or_no_full_pair_is_not_judged(self):
        env = rustc_env("1.99.1", engines_selected=["v1_2_2", "redis"])
        self.assertEqual(compare.rustc_check(env), ([], []))
        env = rustc_env("1.100.0")
        del env["engines"]["candidate_full"]  # --candidate-full not given
        self.assertEqual(compare.rustc_check(env), ([], []))

    def test_the_rustc_is_recorded(self):
        b, res = build([("s/c", "x_rps", "req/s", "higher", 1.0, 1.0, 1.0, 1.0)])
        env = rustc_env("1.100.0", cand_full="1.101.0")
        compare.record(b, res, env, "lbl", False)
        rec = b["metrics"]["s/c/x_rps"]["recorded"]
        self.assertEqual((rec["rustc"], rec["rustc_full"]), ("1.100.0", "1.101.0"))
        self.assertEqual(b["pins"].get("v1_2_2_full_sha256"), None)  # no sha in this env


class SkippedAndNotRun(unittest.TestCase):
    """A configuration skipped for want of --candidate-full, and recorded
    metrics that did not run."""

    def test_not_run_lists_recorded_metrics_without_results(self):
        b, res = build([("s/a", "x_ms", "ms", "lower", 1.0, 1.0, 1.0, 1.0),
                        ("s/lua", "y_rps", "req/s", "higher", 1.0, 1.0, 1.0, 1.0)])
        del res["s/lua/y_rps"]
        rows, not_run, _ = compare.evaluate(res, b)
        self.assertEqual(not_run, ["s/lua/y_rps"])
        self.assertEqual([r["key"] for r in rows], ["s/a/x_ms"])

    def test_a_skipped_configuration_is_not_expected_and_is_shown(self):
        import tempfile
        b, res = build([("s/a", "x_ms", "ms", "lower", 1.0, 1.0, 1.0, 1.0),
                        ("s/lua", "y_rps", "req/s", "higher", 1.0, 1.0, 1.0, 1.0)])
        del res["s/lua/y_rps"]
        env = {"expected": [{"suite": "s", "config": "a", "engines": ["v1_2_2", "candidate"]}],
               "engines_selected": ["v1_2_2", "candidate"], "skipped_configs": ["s/lua"]}
        # Not expected: missing values of the skipped configuration are not INVALID ...
        self.assertEqual(compare.validity_problems(env, b, res, False, False), [])
        rows, not_run, hf = compare.evaluate(res, b)
        with tempfile.TemporaryDirectory() as d:
            summ = compare.write_summary(d, rows, not_run, env, "lbl", [], False, hf, b)
            md = open(os.path.join(d, "summary.md")).read()
        # ... but it is named, and its metrics are "not run" (which the release gate refuses).
        self.assertIn("skipped (need --candidate-full): s/lua", md)
        self.assertEqual(summ["not_run"], ["s/lua/y_rps"])
        self.assertEqual(summ["verdict"], "ok")


# The release gate (scripts/release/regression_gate.py), without git.
_GATE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", "..", "scripts", "release")
sys.path.insert(0, os.path.abspath(_GATE))
import regression_gate  # noqa: E402

SUITES = ["01-small", "18-big"]


def small_env(**kw):
    env = {"vm": "Standard_D4s_v5", "nproc": 4, "scale": "1", "only": "", "full": "1",
           "engines_selected": ["v1_2_2", "candidate", "redis"],
           "suite_seconds": {"01-small": 10}, "needs_more_cpus": {"18-big": 8},
           "suite_min_cpus": {"18-big": 8}, "min_cpus_only": False, "skipped_configs": []}
    env.update(kw)
    return env


def big_env(**kw):
    env = small_env(vm="Standard_D8s_v5", nproc=8, suite_seconds={"18-big": 10}, needs_more_cpus={},
                    min_cpus_only=True)
    env.update(kw)
    return env


OK_SUMMARY = {"verdict": "ok", "failed": [], "problems": []}


class MinCpus(unittest.TestCase):
    """Suites that declare Suite(..., min_cpus=N)."""

    def baseline(self):
        b, _ = build([("01-small/c", "x_rps", "req/s", "higher", 1.0, 1.0, 1.0, 1.0),
                      ("18-big/c", "y_rps", "req/s", "higher", 1.0, 1.0, 1.0, 1.0)])
        b["metrics"]["01-small/c/x_rps"]["recorded"]["vm"] = "Standard_D4s_v5"
        b["metrics"]["18-big/c/y_rps"]["recorded"]["vm"] = "Standard_D8s_v5"
        return b

    def results(self, *keys):
        _, res = build([("01-small/c", "x_rps", "req/s", "higher", 1.0, 1.0, 1.0, 1.0),
                        ("18-big/c", "y_rps", "req/s", "higher", 1.0, 1.0, 1.0, 1.0)])
        return {k: v for k, v in res.items() if k in keys}

    def test_a_suite_needing_more_cpus_on_a_small_machine_is_not_run_not_invalid(self):
        b, res = self.baseline(), self.results("01-small/c/x_rps")
        env = small_env(expected=[{"suite": "01-small", "config": "c", "engines": ["v1_2_2", "candidate"]}])
        self.assertEqual(compare.validity_problems(env, b, res, False, False), [])
        _, not_run, _ = compare.evaluate(res, b)
        self.assertEqual(not_run, ["18-big/c/y_rps"])

    def test_each_suite_is_judged_on_the_machine_it_was_recorded_on(self):
        b = self.baseline()
        env = big_env(expected=[{"suite": "18-big", "config": "c", "engines": ["v1_2_2", "candidate"]}])
        self.assertEqual(compare.validity_problems(env, b, self.results("18-big/c/y_rps"), False, False), [])
        # A suite recorded on the D4s_v5 run on the D8s_v5 is not comparable.
        probs = compare.validity_problems(env, b, self.results("01-small/c/x_rps"), False, False)
        self.assertTrue(any("Standard_D4s_v5 that 01-small was recorded on" in p for p in probs), probs)

    def test_run_below_min_cpus_via_only_is_invalid_and_refused_by_the_gate(self):
        b, res = self.baseline(), self.results("18-big/c/y_rps")
        env = small_env(only="18", suite_seconds={"18-big": 10}, needs_more_cpus={},
                        ran_below_min_cpus={"18-big": 8})
        probs = compare.validity_problems(env, b, res, False, False)
        self.assertTrue(any("18-big needs 8 CPUs and ran on 4: not valid for the gate" in p for p in probs), probs)
        why, covers = regression_gate.judge(env, OK_SUMMARY, SUITES)
        self.assertTrue(any("fewer CPUs than they declare" in w for w in why), why)
        self.assertEqual(covers, set())

    def test_the_release_gate_needs_both_runs(self):
        small = regression_gate.judge(small_env(), dict(OK_SUMMARY, not_run=["18-big/c/y_rps"]), SUITES)
        self.assertEqual(small, ([], {"01-small"}))
        # Only the small run: refused, naming the suite.
        used, uncovered = regression_gate.coverage(SUITES, {"d4": small})
        self.assertEqual(uncovered, ["18-big"])
        # With the D8s_v5 --min-cpus-only run: every suite is covered.
        big = regression_gate.judge(big_env(), dict(OK_SUMMARY, not_run=["01-small/c/x_rps"]), SUITES)
        self.assertEqual(big, ([], {"18-big"}))
        used, uncovered = regression_gate.coverage(SUITES, {"d4": small, "d8": big})
        self.assertEqual((used, uncovered), (["d4", "d8"], []))
        self.assertEqual(regression_gate.needs([small_env()]), {"18-big": 8})

    def test_a_min_cpus_run_on_a_machine_too_small_covers_nothing(self):
        why, covers = regression_gate.judge(big_env(nproc=4), OK_SUMMARY, SUITES)
        self.assertEqual((why, covers), ([], set()))

    def test_the_gate_refuses_other_partial_runs(self):
        # A metric not run for another reason than needs_more_cpus.
        why, _ = regression_gate.judge(small_env(needs_more_cpus={}), dict(OK_SUMMARY, not_run=["18-big/c/y_rps"]),
                                       SUITES)
        self.assertTrue(any("not run" in w for w in why), why)
        why, _ = regression_gate.judge(small_env(skipped_configs=["01-small/lua"]), OK_SUMMARY, SUITES)
        self.assertTrue(any("--candidate-full" in w for w in why), why)
        why, _ = regression_gate.judge(small_env(only="01"), OK_SUMMARY, SUITES)
        self.assertTrue(any("partial run" in w for w in why), why)
        why, _ = regression_gate.judge(big_env(full="0"), OK_SUMMARY, SUITES)
        self.assertTrue(any("FULL=0" in w for w in why), why)
        why, _ = regression_gate.judge(small_env(), dict(OK_SUMMARY, verdict="INVALID"), SUITES)
        self.assertTrue(why)

    def test_a_small_group_is_scaled_by_its_own_recording_hosts_factor(self):
        # Five suites recorded on an older CPU, now 1.2x faster; one suite of
        # two metrics recorded on THIS host: it must be judged at its host's
        # 1.0, not the global 1.2 (which would fail it).
        specs = [("old%d/c" % i, "t_ms", "ms", "lower", 12.0, 10.0, 10.0, 10.0 / 1.2 * 1.05) for i in range(5)]
        specs += [("new/c", "a_rps", "req/s", "higher", 100.0, 100.0, 100.0, 95.0),
                  ("new/c", "b_rps", "req/s", "higher", 100.0, 100.0, 100.0, 95.0)]
        b, res = build(specs)
        for k, m in b["metrics"].items():
            m["recorded"].update({"vm": "D4", "cpu": "new" if k.startswith("new/") else "old"})
        # The new host's own metrics are too few for a host factor: add a third.
        specs.append(("new2/c", "c_rps", "req/s", "higher", 100.0, 100.0, 100.0, 100.0))
        b2, res2 = build(specs)
        for k, m in b2["metrics"].items():
            m["recorded"].update({"vm": "D4", "cpu": "new" if k.startswith("new") else "old"})
        rows, _, hf = compare.evaluate(res2, b2)
        st = {r["key"]: r for r in rows}
        self.assertAlmostEqual(hf["global"], 1.2, places=1)
        self.assertEqual(st["new/c/a_rps"]["host"]["mode"], "host")
        self.assertAlmostEqual(st["new/c/a_rps"]["host"]["speed"], 1.0)
        self.assertEqual(st["new/c/a_rps"]["status"], "pass")

    def test_the_gate_uses_the_code_check(self):
        why, covers = regression_gate.judge(small_env(), OK_SUMMARY, SUITES, lambda env: ["code changed"])
        self.assertEqual((why, covers), (["code changed"], set()))


class GateStrict(unittest.TestCase):
    """What the release gate must not let through (second review of #921)."""

    def baseline(self):
        b, _ = build([("01-small/c", "x_rps", "req/s", "higher", 1.0, 100.0, 1.0, 100.0)])
        b["metrics"]["01-small/c/x_rps"]["recorded"]["vm"] = "Standard_D4s_v5"
        return b

    def env(self, **kw):
        e = small_env(needs_more_cpus={}, suite_min_cpus={}, suite_seconds={"01-small": 1},
                      expected=[{"suite": "01-small", "config": "c", "engines": ["v1_2_2", "candidate"]}],
                      engines_selected=["v1_2_2", "candidate", "redis"])
        e.update(kw)
        return e

    def test_an_unrecorded_suite_is_refused(self):
        self.assertEqual(regression_gate.unrecorded(["01-small", "26-new"], self.baseline()), ["26-new"])
        self.assertEqual(regression_gate.unrecorded(["01-small"], self.baseline()), [])

    def test_rows_that_are_new_are_refused(self):
        summ = dict(OK_SUMMARY, rows=[{"key": "01-small/c/x_rps", "status": "new"}])
        why, covers = regression_gate.judge(self.env(), summ, ["01-small"])
        self.assertTrue(any("no recorded value" in w and "01-small/c/x_rps" in w for w in why), why)
        self.assertEqual(covers, set())

    def test_each_override_is_refused(self):
        for key, text in (("rustc_override", "REGRESS_ALLOW_RUSTC"), ("allow_vm_change", "ALLOW_VM_CHANGE")):
            why, covers = regression_gate.judge(self.env(**{key: True}), OK_SUMMARY, ["01-small"])
            self.assertTrue(any(text in w for w in why), (key, why))
            self.assertEqual(covers, set())

    def test_a_run_on_another_machine_size_is_refused(self):
        why, _ = regression_gate.judge(self.env(vm="Standard_D8s_v5"), OK_SUMMARY, ["01-small"], baseline=self.baseline())
        self.assertTrue(any("recorded on another machine size: 01-small (Standard_D4s_v5)" in w for w in why), why)
        why, covers = regression_gate.judge(self.env(), OK_SUMMARY, ["01-small"], baseline=self.baseline())
        self.assertEqual((why, covers), ([], {"01-small"}))

    def test_the_run_is_judged_again_not_trusted(self):
        # A committed summary.json may say ok; the raw results decide.
        b = self.baseline()
        _, res = build([("01-small/c", "x_rps", "req/s", "higher", 1.0, 100.0, 1.0, 50.0)])
        summ = regression_gate.rejudge(compare, self.env(), res, b)
        self.assertEqual((summ["verdict"], summ["failed"]), ("REGRESSED", ["01-small/c/x_rps"]))
        why, covers = regression_gate.judge(self.env(), summ, ["01-small"], baseline=b)
        self.assertTrue(why)
        self.assertEqual(covers, set())
        _, res = build([("01-small/c", "x_rps", "req/s", "higher", 1.0, 100.0, 1.0, 100.0)])
        self.assertEqual(regression_gate.rejudge(compare, self.env(), res, b)["verdict"], "ok")

    def test_rejudging_ignores_the_rustc_override(self):
        _, res = build([("01-small/c", "x_rps", "req/s", "higher", 1.0, 100.0, 1.0, 100.0)])
        env = self.env(rustc_override=True, engines={"v1_2_2": {"rustc": "1.100.0"}, "candidate": {"rustc": "1.99.1"}})
        summ = regression_gate.rejudge(compare, env, res, self.baseline())
        self.assertEqual(summ["verdict"], "INVALID")
        self.assertTrue(any("older than" in p for p in summ["problems"]), summ["problems"])

    def test_a_redis_built_from_the_pinned_tarball_is_accepted(self):
        b = self.baseline()
        b["pins"] = {"redis_sha256": "image", "redis_source_sha256": "tarball"}
        env = self.env(engines={"redis": {"sha256": "built-here"}})
        probs = compare.validity_problems(dict(env, redis_source_sha256="tarball"), b, {"x": {}}, False, False)
        self.assertFalse(any("redis-server" in p for p in probs), probs)
        probs = compare.validity_problems(dict(env, redis_source_sha256="other"), b, {"x": {}}, False, False)
        self.assertTrue(any("redis-server" in p for p in probs), probs)


if __name__ == "__main__":
    unittest.main()
