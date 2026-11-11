import csv, sys
D = sys.argv[1]
S = {(r["engine"], r["scenario"]): r for r in csv.DictReader(open(D + "/summary.csv"))}
R = list(csv.DictReader(open(D + "/runs.csv")))
for sc in sys.argv[2:]:
    n, o = S[("kividb-121", sc)], S[("kividb-120rel", sc)]
    ch = 100 * (float(n["ops_median"]) / float(o["ops_median"]) - 1)
    rr = lambda e: sorted(float(r["ops"]) for r in R if r["engine"] == e and r["scenario"] == sc and r["ops"])
    rn, ro = rr("kividb-121"), rr("kividb-120rel")
    print(f"| {sc} | {float(n['ops_median']):,.0f} [{rn[0]:,.0f}-{rn[-1]:,.0f}] | {float(o['ops_median']):,.0f} [{ro[0]:,.0f}-{ro[-1]:,.0f}] | {ch:+.1f}% | "
          f"{float(n['p50_ms']):.2f}/{float(o['p50_ms']):.2f} | {float(n['p99_ms']):.2f}/{float(o['p99_ms']):.2f} | {float(n['p999_ms']):.2f}/{float(o['p999_ms']):.2f} | "
          f"{n['us_per_op']}/{o['us_per_op']} | {n['limit'] or '-'} | {n['rounds']}/{o['rounds']} |")
