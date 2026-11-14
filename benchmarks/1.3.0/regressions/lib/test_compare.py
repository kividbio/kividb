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
        self.assertEqual(row["host"]["mode"], "suite")

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


if __name__ == "__main__":
    unittest.main()
