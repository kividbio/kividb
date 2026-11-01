"""
Compares a real vector-db-benchmark (redis/vector-db-benchmark, Rust tool) run of kividb
AND Redis, on the same host/instance, against
benchmarks/vector-baseline.json. Fails (non-zero exit) if either:

  (a) kividb's own numbers regressed vs. its historical baseline
      ("kividb_absolute" gate -- generous tolerance, since absolute
      numbers vary across CI instance types over time), or
  (b) kividb's competitive ratio against Redis (computed
      fresh from this run, same instance, same moment -- much less noisy
      than (a)) dropped below the floor in "kividb_vs_redis_ratio".

One dataset per invocation. Where its gates and numbers live in the baseline
file depends on the dataset:

  - glove-25-angular (the original, unfiltered dataset) uses the file's
    TOP-LEVEL "kividb"/"redis"/"gates" blocks, exactly as before datasets
    were added. Its search results are keyed by `ef` alone
    ("search_by_ef.<ef>.<metric>"), collapsing parallel configs at the same
    `ef` to the higher-RPS one (parallel=100, by construction of the
    workflow's engines-files).
  - Every other dataset has its own section, "datasets.<name>", with the same
    "kividb"/"redis"/"gates" shape. These are run with several client counts
    at the same `ef`, so their results are keyed by both:
    "search.ef<ef>_p<parallel>.<metric>" (metrics: rps, p50_ms, p95_ms,
    recall, precision). A section may have no "kividb"/"redis" numbers yet:
    its kividb_absolute gates then report "NO BASELINE" and do not fail, and
    the first `update_baseline` run fills them in. The ratio gates need no
    stored numbers and always apply.

Reads the tool's own two native output files per engine (found in its
--host-relative `results/` directory):

  - `<experiment>-<dataset>-upload-*.json` (most recent by mtime): ingestion.
  - `<experiment>-<dataset>-summary.json`: search, from the flat
    `search_results` array. The tool's own `--repetitions` (default 3) already
    reports only the best-RPS run per search config.

Gate kinds:
  kividb_absolute:  regression_if below|above, with tolerance_pct (relative)
                    or tolerance_abs (absolute, e.g. 0.01 of recall).
  kividb_vs_redis_ratio:
                    kind kividb_over_redis / redis_over_kividb with min_ratio,
                    or kind kividb_minus_redis with min_delta (e.g. recall
                    may be at most 0.01 below Redis's: min_delta -0.01).

Usage:
    python compare_vector_baseline.py \
        --kividb-experiment kividb-glove25 --kividb-results-dir PATH \
        --redis-experiment redis-glove25 --redis-results-dir PATH \
        --dataset glove-25-angular [--baseline PATH] [--summary-out PATH]

Exit code 0 = no regression. Exit code 1 = at least one gate failed.
Exit code 2 = usage/file error.
"""
from __future__ import annotations

import argparse
import json
import pathlib
import sys


def _latest(paths: list[pathlib.Path]) -> pathlib.Path | None:
    return max(paths, key=lambda p: p.stat().st_mtime) if paths else None


def load_tool_results(results_dir: pathlib.Path, experiment: str, dataset: str,
                      by_parallel: bool = False) -> dict:
    """Returns {"ingest": {...}, "search_by_ef": {ef: {...}}}, or with
    by_parallel {"ingest": {...}, "search": {"ef<ef>_p<parallel>": {...}}}."""
    upload_path = _latest(list(results_dir.glob(f"{experiment}-{dataset}-upload-*.json")))
    if upload_path is None:
        raise FileNotFoundError(
            f"no {experiment}-{dataset}-upload-*.json in {results_dir} "
            "(did the run include the upload/configure stage?)"
        )
    upload_data = json.loads(upload_path.read_text())
    ingest = {
        "upload_time_s": upload_data["results"]["upload_time"],
        "total_time_s": upload_data["results"]["total_time"],
    }

    summary_path = results_dir / f"{experiment}-{dataset}-summary.json"
    if not summary_path.exists():
        raise FileNotFoundError(f"missing {summary_path}")
    summary = json.loads(summary_path.read_text())

    if by_parallel:
        search: dict[str, dict] = {}
        for run in summary.get("search_results", []):
            ef = run.get("ef")
            if ef is None:
                continue
            key = f"ef{int(ef)}_p{int(run['parallel'])}"
            # Newer tool versions split upstream's `mean_precisions` into
            # mean_recall and mean_precision_at_returned; take whichever exists.
            recall = run.get("mean_recall", run.get("mean_precisions"))
            candidate = {
                "rps": run["rps"],
                "p50_ms": run["p50_time"] * 1000,
                "p95_ms": run["p95_time"] * 1000,
                "recall": recall,
                "precision": run.get("mean_precision_at_returned", recall),
            }
            if key not in search or candidate["rps"] > search[key]["rps"]:
                search[key] = candidate
        if not search:
            raise ValueError(f"no search results with an `ef` param found in {summary_path}")
        return {"ingest": ingest, "search": search}

    search_by_ef: dict[int, dict] = {}
    for run in summary.get("search_results", []):
        ef = run.get("ef")
        if ef is None:
            continue
        candidate = {
            "rps": run["rps"],
            "p50_ms": run["p50_time"] * 1000,
            "p95_ms": run["p95_time"] * 1000,
            "precision": run["mean_precisions"],
        }
        if ef not in search_by_ef or candidate["rps"] > search_by_ef[ef]["rps"]:
            search_by_ef[ef] = candidate

    if not search_by_ef:
        raise ValueError(f"no search results with an `ef` param found in {summary_path}")

    return {"ingest": ingest, "search_by_ef": search_by_ef}


def _get(d: dict | None, dotted_key: str):
    """search_by_ef is keyed by int; dotted_key parts are always strings."""
    cur = d
    for part in dotted_key.split("."):
        if not isinstance(cur, dict):
            return None
        if part in cur:
            cur = cur[part]
        elif part.isdigit() and int(part) in cur:
            cur = cur[int(part)]
        else:
            return None
    return cur


def compare_absolute(gates: dict, kividb_results: dict) -> list[dict]:
    rows = []
    for metric_key, spec in gates.items():
        if metric_key.startswith("_"):
            continue
        base_val = spec["baseline"]
        direction = spec["regression_if"]

        actual = _get(kividb_results, metric_key)
        if actual is None:
            rows.append({"check": f"kividb.{metric_key}", "baseline": base_val, "actual": None,
                         "delta_pct": None, "status": "MISSING"})
            continue
        if base_val is None:
            # No recorded baseline yet (a dataset section added before its
            # first update_baseline run): report, don't gate.
            rows.append({"check": f"kividb.{metric_key}", "baseline": None, "actual": actual,
                         "delta_pct": None, "status": "NO BASELINE"})
            continue

        if "tolerance_abs" in spec:
            margin = spec["tolerance_abs"]
            low, high = base_val - margin, base_val + margin
        else:
            tolerance_pct = spec["tolerance_pct"]
            low, high = base_val * (1 - tolerance_pct / 100), base_val * (1 + tolerance_pct / 100)

        delta_pct = (actual - base_val) / base_val * 100 if base_val else 0.0
        if direction == "below":
            regressed = actual < low
        elif direction == "above":
            regressed = actual > high
        else:
            raise ValueError(f"{metric_key}: unknown regression_if {direction!r}")

        rows.append({
            "check": f"kividb.{metric_key}", "baseline": base_val, "actual": actual,
            "delta_pct": delta_pct, "status": "REGRESSED" if regressed else "ok",
        })
    return rows


def compare_ratio(gates: dict, kividb_results: dict, redis_results: dict) -> list[dict]:
    rows = []
    for metric_key, spec in gates.items():
        if metric_key.startswith("_"):
            continue
        kividb_val = _get(kividb_results, metric_key)
        redis_val = _get(redis_results, metric_key)
        kind = spec["kind"]
        floor = spec["min_delta"] if kind == "kividb_minus_redis" else spec["min_ratio"]

        if kividb_val is None or redis_val is None:
            rows.append({"check": f"ratio.{metric_key}", "min_ratio": floor, "actual": None,
                         "status": "MISSING"})
            continue

        if kind == "kividb_over_redis":
            ratio = kividb_val / redis_val if redis_val else float("inf")
        elif kind == "redis_over_kividb":
            ratio = redis_val / kividb_val if kividb_val else float("inf")
        elif kind == "kividb_minus_redis":
            ratio = kividb_val - redis_val
        else:
            raise ValueError(f"{metric_key}: unknown kind {kind!r}")

        regressed = ratio < floor
        rows.append({
            "check": f"ratio.{metric_key}" if kind != "kividb_minus_redis" else f"delta.{metric_key}",
            "min_ratio": floor, "actual": round(ratio, 3 if kind != "kividb_minus_redis" else 4),
            "kividb": kividb_val, "redis": redis_val,
            "status": "REGRESSED" if regressed else "ok",
        })
    return rows


def render_markdown(abs_rows: list[dict], ratio_rows: list[dict]) -> str:
    lines = ["### kividb vs its own baseline", "",
             "| Check | Baseline | Actual | Delta | Status |", "|---|---:|---:|---:|---|"]
    for r in abs_rows:
        if r["actual"] is None:
            base = "--" if r["baseline"] is None else r["baseline"]
            lines.append(f"| `{r['check']}` | {base} | -- | -- | MISSING |")
            continue
        if r["baseline"] is None:
            lines.append(f"| `{r['check']}` | -- | {round(r['actual'], 4)} | -- | {r['status']} |")
            continue
        sign = "+" if r["delta_pct"] >= 0 else ""
        lines.append(f"| `{r['check']}` | {r['baseline']} | {round(r['actual'], 4)} | "
                     f"{sign}{r['delta_pct']:.1f}% | {r['status']} |")

    lines += ["", "### kividb vs Redis (this run, same instance)", "",
              "| Check | kividb | redis | Ratio | Floor | Status |",
              "|---|---:|---:|---:|---:|---|"]
    for r in ratio_rows:
        if r["actual"] is None:
            lines.append(f"| `{r['check']}` | -- | -- | -- | {r['min_ratio']} | MISSING |")
            continue
        lines.append(f"| `{r['check']}` | {round(r['kividb'], 3)} | {round(r['redis'], 3)} | "
                     f"{r['actual']} | {r['min_ratio']} | {r['status']} |")
    return "\n".join(lines)


def render_headline(engines: dict, dataset: str) -> str:
    """Every engine's numbers side by side, gated or not: ingest time first
    (a headline metric for all engines), then each search config."""
    present = {name: r for name, r in engines.items() if r is not None}
    lines = [f"### All engines on {dataset} (this run)", ""]
    for name, r in engines.items():
        if r is None:
            lines.append(f"- **{name}**: no results (its run failed or was skipped; "
                         "report-only, not a gate)")
    if not present:
        return "\n".join(lines)
    if len(present) != len(engines):
        lines.append("")
    names = list(present)
    lines += ["| Metric | " + " | ".join(names) + " |", "|---" + "|---:" * len(names) + "|"]
    for key in ("upload_time_s", "total_time_s"):
        vals = [present[n]["ingest"].get(key) for n in names]
        lines.append(f"| ingest {key} | "
                     + " | ".join("--" if v is None else f"{v:.1f}" for v in vals) + " |")
    search_key = "search" if any("search" in r for r in present.values()) else "search_by_ef"
    configs: list = []
    for r in present.values():
        for k in r.get(search_key, {}):
            if k not in configs:
                configs.append(k)

    def order(k):
        if search_key == "search":
            ef, par = str(k)[2:].split("_p")
            return (int(par), int(ef))
        return (0, int(k))

    configs.sort(key=order)
    metric_names = ("rps", "recall", "p95_ms") if search_key == "search" else ("rps", "precision", "p95_ms")
    for k in configs:
        label = k if search_key == "search" else f"ef{k}"
        for m in metric_names:
            vals = [present[n].get(search_key, {}).get(k, {}).get(m) for n in names]
            digits = 4 if m in ("recall", "precision") else 1
            lines.append(f"| {label} {m} | "
                         + " | ".join("--" if v is None else f"{v:.{digits}f}" for v in vals) + " |")
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--kividb-experiment", required=True)
    ap.add_argument("--kividb-results-dir", type=pathlib.Path, required=True)
    ap.add_argument("--redis-experiment", required=True)
    ap.add_argument("--redis-results-dir", type=pathlib.Path, required=True)
    ap.add_argument("--dataset", required=True, help="e.g. glove-25-angular")
    ap.add_argument("--baseline", type=pathlib.Path,
                     default=pathlib.Path(__file__).parents[3] / "benchmarks" / "vector-baseline.json")
    ap.add_argument("--summary-out", type=pathlib.Path, default=None)
    ap.add_argument("--dump-results-json", type=pathlib.Path, default=None,
                     help="Write {kividb: {...}, redis: {...}} here (used by update-baseline)")
    # An engine that is reported, never gated (Dragonfly). Its results may be
    # absent -- a failed or skipped run -- without failing the compare.
    ap.add_argument("--report-engine", action="append", default=[], metavar="NAME=EXPERIMENT=DIR",
                     help="Report-only engine, e.g. dragonfly=dragonfly-glove25=/tmp/results")
    args = ap.parse_args()

    if not args.baseline.exists():
        print(f"::error::baseline file not found: {args.baseline}", file=sys.stderr)
        return 2

    baseline = json.loads(args.baseline.read_text())
    # Which block of the baseline file holds this dataset (see the module
    # docstring): its own "datasets.<name>" section, or the top level for the
    # original dataset.
    if args.dataset in baseline.get("datasets", {}):
        section = baseline["datasets"][args.dataset]
        by_parallel = True
    elif args.dataset == baseline.get("methodology", {}).get("dataset"):
        section = baseline
        by_parallel = False
    else:
        print(f"::error::{args.baseline} has no gates for dataset {args.dataset!r} "
              f"(add a datasets.{args.dataset} section)", file=sys.stderr)
        return 2

    try:
        kividb_results = load_tool_results(args.kividb_results_dir, args.kividb_experiment,
                                           args.dataset, by_parallel)
        redis_results = load_tool_results(args.redis_results_dir, args.redis_experiment,
                                          args.dataset, by_parallel)
    except (FileNotFoundError, ValueError, KeyError) as e:
        print(f"::error::failed to load benchmark results: {e}", file=sys.stderr)
        return 2

    if args.dump_results_json:
        def dumpable(r):
            if by_parallel:
                return r
            return {"ingest": r["ingest"], "search_by_ef": {str(k): v for k, v in r["search_by_ef"].items()}}
        args.dump_results_json.write_text(json.dumps(
            {"kividb": dumpable(kividb_results), "redis": dumpable(redis_results)}, indent=2))

    abs_gates = {**section["gates"]["kividb_absolute"]}
    for k in list(abs_gates.keys()):
        if k.startswith("_"):
            continue
        abs_gates[k] = {**abs_gates[k], "baseline": _get(section.get("kividb"), k)}
    ratio_gates = section["gates"]["kividb_vs_redis_ratio"]

    reported: dict = {"kividb": kividb_results, "redis": redis_results}
    for spec in args.report_engine:
        name, experiment, results_dir = spec.split("=", 2)
        try:
            reported[name] = load_tool_results(pathlib.Path(results_dir), experiment,
                                               args.dataset, by_parallel)
        except (FileNotFoundError, ValueError, KeyError) as e:
            print(f"::warning::{name} has no usable results for {args.dataset}: {e}", file=sys.stderr)
            reported[name] = None

    abs_rows = compare_absolute(abs_gates, kividb_results)
    ratio_rows = compare_ratio(ratio_gates, kividb_results, redis_results)
    any_regression = any(r["status"] == "REGRESSED" for r in abs_rows + ratio_rows)

    table = render_markdown(abs_rows, ratio_rows) + "\n\n" + render_headline(reported, args.dataset)
    print(f"\nVector benchmark vs. baseline ({args.dataset}):\n")
    print(table)
    print()

    if args.summary_out:
        header = "## Vector benchmark results\n\n" if not by_parallel \
            else f"## Vector benchmark results: {args.dataset}\n\n"
        if any(r["status"] == "NO BASELINE" for r in abs_rows):
            header += ("No kividb baseline recorded for this dataset yet: the "
                       "kividb-vs-baseline rows are report-only until an "
                       "`update_baseline` run records one.\n\n")
        if any_regression:
            header += "**Regression detected** — see REGRESSED rows below.\n\n"
        with open(args.summary_out, "a", encoding="utf-8") as f:
            f.write(header + table + "\n")

    if any_regression:
        print("RESULT: regression detected against benchmarks/vector-baseline.json", file=sys.stderr)
        return 1

    print("RESULT: no regression")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
