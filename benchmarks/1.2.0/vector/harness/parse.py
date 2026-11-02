"""Turn the raw run folders into one summary JSON.

Layout read: <raw>/<pass>/<engine>/<dataset>/{tool.log, mem.csv, results/*.json, client-mpstat.txt}

Per run it extracts
  ingest: upload_s and index_wait_s as the tool reports them, plus the
          load window (tool's "Starting upload" .. "Indexing complete" lines,
          timestamped on the client) mapped onto the server's memory samples;
  memory: idle, peak and post-load RSS and used_memory, bytes per vector;
  search: for every (ef, parallel) point the tool's best-of-3 row (rps,
          recall, p50/p95/p99 latency from the result JSON) and all three
          repetitions' q/s from the log (median and spread);
  client CPU: peak busy % of the client during the run (mpstat).
Usage: python parse.py <raw dir> <out json>
"""
import csv, glob, json, os, re, statistics, sys

TS = re.compile(r"^(\d+\.\d+) (.*)$")


def read_log(path):
    rows = []
    with open(path, encoding="utf-8", errors="replace") as f:
        for line in f:
            m = TS.match(line.rstrip("\n"))
            if m:
                rows.append((float(m.group(1)), m.group(2)))
    return rows


def parse_log(rows):
    out = {"searches": []}
    cur = None
    for t, l in rows:
        if "Starting upload" in l:
            out["load_start"] = t
        m = re.search(r"Upload time: ([\d.]+)s", l)
        if m:
            out["upload_s"] = float(m.group(1))
            out["upload_end"] = t
        if l.lstrip().startswith("Indexing complete") or "Index time:" in l:
            out["index_end"] = t
            m = re.search(r"Index time: ([\d.]+)s", l)
            if m:
                out["index_wait_s"] = float(m.group(1))
            m = re.search(r"Total time \(read\+upload\+index\): ([\d.]+)s", l)
            if m:
                out["total_s"] = float(m.group(1))
        m = re.search(r"Running search \d+: ef=(\d+), parallel=(\d+)", l)
        if m:
            cur = {"ef": int(m.group(1)), "parallel": int(m.group(2)), "reps": [], "t0": t}
            out["searches"].append(cur)
        m = re.search(r"rep \d+/\d+: QPS ([\d.]+)", l)
        if m and cur is not None:
            cur["reps"].append(float(m.group(1)))
        m = re.search(r"QPS: ([\d.]+), Recall: ([\d.]+)", l)
        if m and cur is not None:
            cur["best_rps"] = float(m.group(1))
            cur["recall"] = float(m.group(2))
            cur["t1"] = t
        if "Experiment stage: Cleanup" in l:
            out["cleanup"] = t
        if " EXIT " in f" {l} ":
            out["exit"] = l.split()[-1]
    return out


def read_mem(path):
    rows = []
    if not os.path.exists(path):
        return rows
    with open(path) as f:
        for r in csv.DictReader(f):
            try:
                rows.append({
                    "t": float(r["t"]),
                    "rss": int(r["rss_kb"]) * 1024 if r["rss_kb"] else None,
                    "used": int(r["used_memory"]) if r["used_memory"] else None,
                    "keys": int(r["dbsize"]) if r["dbsize"] else None,
                })
            except ValueError:
                pass
    return rows


def search_json(results_dir):
    """Map (ef, parallel) -> latency fields from the tool's search result files."""
    found = {}
    for p in glob.glob(os.path.join(results_dir, "**", "*.json"), recursive=True):
        if "-search-" not in os.path.basename(p):
            continue
        try:
            d = json.load(open(p))
        except Exception:
            continue
        params = d.get("params", {})
        res = d.get("results", {})
        ef = (params.get("search_params") or {}).get("ef") or params.get("ef")
        par = params.get("parallel")
        if ef is None or par is None:
            continue
        found[(int(ef), int(par))] = {
            k: res.get(k) for k in ("rps", "p50_time", "p95_time", "p99_time", "mean_time",
                                    "mean_recall", "client_saturated", "client_cpu_cores_used", "total_time")
        }
    return found


def mpstat_peak(path):
    peak = 0.0
    if not os.path.exists(path):
        return None
    for line in open(path, errors="replace"):
        parts = line.split()
        if len(parts) > 3 and parts[1] == "all" or (len(parts) > 3 and parts[2] == "all"):
            try:
                idle = float(parts[-1])
                peak = max(peak, 100.0 - idle)
            except ValueError:
                pass
    return peak


def main(raw, out_path):
    runs = []
    for d in sorted(glob.glob(os.path.join(raw, "*", "*", "*"))):
        if not os.path.isdir(d):
            continue
        pas, eng, ds = d.replace("\\", "/").split("/")[-3:]
        logp = os.path.join(d, "tool.log")
        if not os.path.exists(logp):
            continue
        lg = parse_log(read_log(logp))
        mem = read_mem(os.path.join(d, "mem.csv"))
        if lg.get("exit") is None or not mem:
            continue  # still running, or the run died: nothing to summarise
        run = {"pass": pas, "engine": eng, "dataset": ds, "exit": lg.get("exit"),
               "upload_s": lg.get("upload_s"), "index_wait_s": lg.get("index_wait_s"),
               "total_s": lg.get("total_s"),
               "ingest_s": (lg["upload_s"] + lg.get("index_wait_s", 0.0)) if lg.get("upload_s") else None, "client_cpu_peak": mpstat_peak(os.path.join(d, "client-mpstat.txt"))}
        if mem:
            t0 = mem[0]["t"]
            run["mem_t0"] = t0
            run["load_start_rel"] = lg["load_start"] - t0 if "load_start" in lg else None
            run["load_end_rel"] = (lg.get("index_end") or lg.get("upload_end") or t0) - t0
            run["cleanup_rel"] = lg["cleanup"] - t0 if "cleanup" in lg else None
            after = [m for m in mem if "cleanup" in lg and m["t"] > lg["cleanup"] + 3 and m["rss"]]
            if after:  # the tool has deleted the index and data by now
                run["after_delete_rss"], run["after_delete_used"], run["after_delete_keys"] = after[-1]["rss"], after[-1]["used"], after[-1]["keys"]
            idle = mem[0]
            run["idle_rss"], run["idle_used"] = idle["rss"], idle["used"]
            run["peak_rss"] = max((m["rss"] or 0) for m in mem)
            run["peak_used"] = max((m["used"] or 0) for m in mem)
            # post-load: the last sample taken by the time the tool saw indexing
            # complete (+0.6 s: half a sampling interval plus the ~0.1 s clock
            # offset between the machines), i.e. the index fully built and
            # nothing searched or dropped yet.
            end_abs = lg.get("index_end") or lg.get("upload_end")
            post = [m for m in mem if end_abs and m["t"] <= end_abs + 0.6]
            # and, when the run goes on to search, the steady state over the
            # next 10 s (up to the tool's cleanup): median of those samples.
            stop = min(end_abs + 10, lg.get("cleanup", end_abs + 10)) if end_abs else 0
            steady = [m for m in mem if end_abs and end_abs - 0.6 <= m["t"] <= stop and m["keys"]]
            if steady:
                kmax = max(m["keys"] for m in steady)
                steady = [m for m in steady if m["keys"] == kmax]
                post = [{"rss": statistics.median(m["rss"] for m in steady),
                         "used": statistics.median(m["used"] for m in steady), "keys": kmax}]
            if post:
                p = post[-1]
                run["post_rss"], run["post_used"], run["post_keys"] = p["rss"], p["used"], p["keys"]
            run["mem_series"] = [[round(m["t"] - t0, 2), m["rss"], m["used"], m["keys"]] for m in mem]
        js = search_json(os.path.join(d, "results"))
        searches = []
        for s in [x for x in lg["searches"] if x["reps"] and x.get("best_rps")]:
            key = (s["ef"], s["parallel"])
            j = js.get(key, {})
            reps = s["reps"]
            searches.append({
                "ef": s["ef"], "parallel": s["parallel"], "best_rps": s.get("best_rps"),
                "reps": reps, "median_rps": statistics.median(reps) if reps else None,
                "spread_pct": (100 * (max(reps) - min(reps)) / statistics.median(reps)) if len(reps) > 1 else None,
                "recall": s.get("recall"),
                "p50_ms": (j.get("p50_time") or 0) * 1000 or None,
                "p95_ms": (j.get("p95_time") or 0) * 1000 or None,
                "p99_ms": (j.get("p99_time") or 0) * 1000 or None,
                "client_saturated": j.get("client_saturated"),
                "client_cpu_cores": j.get("client_cpu_cores_used"),
            })
        run["searches"] = searches
        runs.append(run)
    json.dump(runs, open(out_path, "w"), indent=1)
    print(f"{len(runs)} runs -> {out_path}")


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
