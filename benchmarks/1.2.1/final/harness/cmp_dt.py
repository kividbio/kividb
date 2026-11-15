import csv, statistics as st, sys
R = list(csv.DictReader(open(sys.argv[1])))
cells = {}
for r in R:
    cells.setdefault((r["command"], r["pipeline"]), {}).setdefault(r["engine"], []).append(r)
print("| Row | 1.2.1 ops/s median [min-max] | 1.2.0 ops/s median [min-max] | Δ | spread 1.2.1 / 1.2.0 | p50 ms | p99 ms | p99.9 ms | server cores | client CPU % |")
print("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
for (c, p), e in sorted(cells.items()):
    n, o = e.get("kividb-121"), e.get("kividb-120")
    if not n or not o: continue
    f = lambda rs, k: [float(x[k]) for x in rs]
    on, oo = f(n, "ops_per_sec"), f(o, "ops_per_sec")
    mn, mo = st.median(on), st.median(oo)
    sp = lambda v: 100 * (max(v) - min(v)) / st.median(v)
    ch = 100 * (mn / mo - 1)
    flag = " **"if abs(ch) > 3 else ""
    print(f"| {c} 100 B p{p} | {mn/1e6:.2f}M [{min(on)/1e6:.2f}-{max(on)/1e6:.2f}] | {mo/1e6:.2f}M [{min(oo)/1e6:.2f}-{max(oo)/1e6:.2f}] | {ch:+.1f}%{flag} | {sp(on):.1f}% / {sp(oo):.1f}% | "
          f"{st.median(f(n,'p50_ms')):.2f} / {st.median(f(o,'p50_ms')):.2f} | {st.median(f(n,'p99_ms')):.2f} / {st.median(f(o,'p99_ms')):.2f} | {st.median(f(n,'p999_ms')):.2f} / {st.median(f(o,'p999_ms')):.2f} | "
          f"{st.median(f(n,'server_cores')):.1f} / {st.median(f(o,'server_cores')):.1f} | {max(f(n,'client_cpu_pct')):.0f} / {max(f(o,'client_cpu_pct')):.0f} |")
