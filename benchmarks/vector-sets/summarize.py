#!/usr/bin/env python3
"""Print a markdown table from vset_bench.py result files.

    python3 summarize.py results/*.json
"""
import json
import sys


def ef_str(ef):
    return "default" if ef is None else str(ef)


def main(paths):
    runs = [json.load(open(p)) for p in paths]
    order = {"kividb": 0, "redis": 1, "redis-noquant": 2}
    runs.sort(key=lambda r: (r["config"]["n"], order.get(r["label"], 9), r["label"]))
    print("| Vectors | Server | Load s | Bytes/vector | EF | Clients | ops/s | p50 ms | p99 ms | recall@10 |")
    print("|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|")
    for r in runs:
        load = r.get("load", {})
        first = True
        for c in r["vsim"]:
            print("| {n} | {label} | {load} | {bpv} | {ef} | {cl} | {ops:,.0f} | {p50:.2f} | {p99:.2f} | {rec:.4f} |".format(
                n=f"{r['config']['n']:,}" if first else "",
                label=r["label"] if first else "",
                load=f"{load.get('seconds', 0):.1f}" if first and load else "",
                bpv=f"{load.get('bytes_per_vector', 0):,.0f}" if first and load else "",
                ef=ef_str(c["ef"]), cl=c["clients"], ops=c["ops_per_sec"],
                p50=c["p50_ms"], p99=c["p99_ms"], rec=c["recall_at_10"]))
            first = False


if __name__ == "__main__":
    main(sys.argv[1:])
